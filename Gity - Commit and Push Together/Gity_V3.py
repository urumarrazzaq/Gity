import os
import logging
import time
import hashlib
from pathlib import Path
from collections import deque
from git import Repo, GitCommandError
from typing import List, Tuple, Optional, Set

# Constants
MAX_CHUNK_SIZE = 25 * 1024 * 1024  # 25MB
MAX_PUSH_RETRIES = 3
MIN_COMMIT_INTERVAL = 30  # seconds between commits to avoid rate limiting
MAX_COMMIT_MESSAGE_LENGTH = 100  # characters

def setup_logging():
    """Enhanced logging setup with rotation"""
    from logging.handlers import RotatingFileHandler
    
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler with rotation (10MB per file, max 5 files)
    file_handler = RotatingFileHandler(
        'ue_git_push.log',
        maxBytes=10*1024*1024,
        backupCount=5
    )
    file_handler.setFormatter(log_formatter)
    
    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, stream_handler]
    )

def get_file_size(file_path: str) -> int:
    """Get size of a file or directory in bytes with error handling"""
    try:
        if os.path.isfile(file_path):
            return os.path.getsize(file_path)
        elif os.path.isdir(file_path):
            return sum(
                os.path.getsize(f) 
                for f in Path(file_path).rglob('*') 
                if f.is_file()
            )
    except (OSError, PermissionError) as e:
        logging.warning(f"Could not get size for {file_path}: {str(e)}")
    return 0

def is_binary_file(file_path: str) -> bool:
    """Check if a file is binary (heuristic)"""
    try:
        with open(file_path, 'rb') as f:
            return b'\0' in f.read(1024)
    except Exception:
        return False

def generate_file_fingerprint(file_path: str) -> str:
    """Generate a fingerprint for file content to detect changes"""
    if not os.path.isfile(file_path):
        return ""
    
    hasher = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return ""

def collect_files_to_process(repo: Repo, path: str, max_size: int) -> Tuple[List[str], int, Optional[str]]:
    """
    Collect files to process, grouping small files together with intelligent sorting
    Returns: (files_to_process, total_size, next_path_to_process)
    """
    items_to_process = []
    total_size = 0
    queue = deque([path])
    
    # Track processed files to avoid duplicates
    processed_files: Set[str] = set()
    
    while queue:
        current_path = queue.popleft()
        
        if os.path.isfile(current_path):
            if current_path in processed_files:
                continue
                
            processed_files.add(current_path)
            file_size = get_file_size(current_path)
            
            if file_size > max_size:
                logging.warning(f"Skipping large file: {current_path} ({file_size/1024/1024:.2f}MB)")
                continue
                
            rel_path = os.path.relpath(current_path, repo.working_dir)
            
            # Skip ignored files
            try:
                if repo.git.check_ignore(rel_path):
                    logging.info(f"Skipping ignored file: {rel_path}")
                    continue
            except GitCommandError:
                logging.info(f"Git check-ignore failed for {rel_path}, including in commit")
            
            # Check if file has actually changed (for modified files)
            if rel_path in [item.a_path for item in repo.index.diff(None)]:
                file_fingerprint = generate_file_fingerprint(current_path)
                try:
                    # Get the last committed version fingerprint
                    old_content = repo.git.show(f"HEAD:{rel_path}")
                    old_fingerprint = hashlib.sha256(old_content.encode()).hexdigest()
                    if file_fingerprint == old_fingerprint:
                        logging.info(f"Skipping unchanged file: {rel_path}")
                        continue
                except GitCommandError:
                    pass  # File is new or other git error
            
            if total_size + file_size <= max_size:
                items_to_process.append(rel_path)
                total_size += file_size
            else:
                return items_to_process, total_size, current_path
        else:
            try:
                # Process directories by sorting files intelligently
                dir_entries = []
                for entry in os.scandir(current_path):
                    if not entry.name.startswith('.'):
                        dir_entries.append(entry)
                
                # Sort files by size (smallest first) and type (source files first)
                dir_entries.sort(key=lambda e: (
                    not e.is_file(),  # Directories last
                    not e.name.endswith(('.cpp', '.h', '.py', '.txt')),  # Source files first
                    get_file_size(e.path) if e.is_file() else 0
                ))
                
                for entry in dir_entries:
                    queue.appendleft(entry.path)  # Process in reverse order due to LIFO
            except PermissionError:
                logging.warning(f"Permission denied for: {current_path}")
    
    return items_to_process, total_size, None

def process_chunk(repo: Repo, chunk: List[str], commit_message: str, remote_name: str = 'origin') -> bool:
    """Add, commit, and push a chunk of files with rate limiting and better error handling"""
    try:
        # Stage files
        repo.index.add(chunk)
        logging.info(f"Staged {len(chunk)} files")
        
        # Truncate commit message if too long
        if len(commit_message) > MAX_COMMIT_MESSAGE_LENGTH:
            commit_message = commit_message[:MAX_COMMIT_MESSAGE_LENGTH-3] + "..."
        
        # Commit
        repo.index.commit(commit_message)
        logging.info(f"Committed: {commit_message}")
        
        # Rate limiting
        time.sleep(MIN_COMMIT_INTERVAL)
        
        # Push with retries and exponential backoff
        for attempt in range(MAX_PUSH_RETRIES):
            try:
                push_result = repo.remote(name=remote_name).push()
                
                # Check if push was successful
                if any('rejected' in str(info.flags) for info in push_result):
                    raise GitCommandError('push', "Push was rejected by remote")
                
                logging.info(f"Successfully pushed chunk: {commit_message}")
                return True
            except GitCommandError as e:
                if attempt < MAX_PUSH_RETRIES - 1:
                    wait_time = (2 ** attempt) * 5  # Exponential backoff
                    logging.warning(
                        f"Push attempt {attempt + 1} failed, retrying in {wait_time}s... Error: {e}"
                    )
                    time.sleep(wait_time)
                    continue
                logging.error(f"Push failed after {MAX_PUSH_RETRIES} attempts: {e}")
                raise
    except Exception as e:
        logging.error(f"Failed to process chunk: {str(e)}")
        return False

def check_remote_connection(repo: Repo, remote_name: str = 'origin') -> bool:
    """Check if we can connect to the remote repository"""
    try:
        remote = repo.remote(name=remote_name)
        remote.fetch()
        return True
    except Exception as e:
        logging.error(f"Failed to connect to remote: {str(e)}")
        return False

def process_repository(repo_path: str, remote_url: str) -> bool:
    """Main processing function with enhanced error handling and progress tracking"""
    try:
        repo = Repo(repo_path)
        
        # Verify repository state
        if repo.bare:
            logging.error("Repository is bare")
            return False
            
        if repo.is_dirty():
            logging.warning("Repository has uncommitted changes that will not be processed")
        
        # Setup remote if needed
        if not repo.remotes:
            repo.create_remote('origin', remote_url)
        
        # Check remote connection
        if not check_remote_connection(repo):
            return False
        
        # Get all files that need processing
        untracked = repo.untracked_files
        changed = [item.a_path for item in repo.index.diff(None)]  # Unstaged changes
        staged = [item.a_path for item in repo.index.diff('HEAD')]  # Staged changes
        
        all_files = set(untracked + changed + staged)
        remaining_paths = [os.path.join(repo.working_dir, f) for f in all_files]
        
        # Process files in chunks
        total_files_processed = 0
        while remaining_paths:
            current_path = remaining_paths.pop(0)
            
            items_to_process, chunk_size, remaining_path = collect_files_to_process(
                repo, current_path, MAX_CHUNK_SIZE
            )
            
            if remaining_path:
                remaining_paths.insert(0, remaining_path)
            
            if items_to_process:
                commit_msg = (
                    f"Added {len(items_to_process)} items (~{chunk_size/1024/1024:.2f}MB) "
                    f"[Total: {total_files_processed + len(items_to_process)}]"
                )
                if not process_chunk(repo, items_to_process, commit_msg):
                    return False
                total_files_processed += len(items_to_process)
                    
        return True
        
    except Exception as e:
        logging.error(f"Error processing repository: {str(e)}")
        return False

def validate_inputs(repo_path: str, remote_url: str) -> bool:
    """Validate user inputs before processing"""
    if not os.path.isdir(repo_path):
        logging.error("Invalid directory path")
        return False
    
    if not remote_url.startswith(('http://', 'https://', 'git@')):
        logging.error("Remote URL should be a valid git repository URL")
        return False
    
    return True

def main():
    """Main function with better user interaction"""
    setup_logging()
    
    print("Git Repository Pusher - Enhanced Version")
    print("--------------------------------------")
    
    repo_path = input("Enter directory path: ").strip()
    remote_url = input("Enter repo link: ").strip()
    
    if not validate_inputs(repo_path, remote_url):
        return
    
    try:
        logging.info(f"Starting to process repository: {repo_path}")
        start_time = time.time()
        
        if process_repository(repo_path, remote_url):
            elapsed_time = time.time() - start_time
            logging.info(
                f"Operation completed successfully in {elapsed_time:.1f} seconds"
            )
        else:
            logging.error("Operation completed with errors")
            
    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
    except Exception as e:
        logging.error(f"Operation failed: {str(e)}")

if __name__ == "__main__":
    main()