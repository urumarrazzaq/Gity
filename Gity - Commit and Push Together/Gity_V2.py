import os
import logging
from pathlib import Path
from collections import deque
from git import Repo, GitCommandError

# Constants
MAX_CHUNK_SIZE = 25 * 1024 * 1024  # 25MB
MAX_PUSH_RETRIES = 3

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ue_git_push.log'),
            logging.StreamHandler()
        ]
    )

def get_file_size(file_path):
    """Get size of a file or directory in bytes"""
    if os.path.isfile(file_path):
        return os.path.getsize(file_path)
    elif os.path.isdir(file_path):
        return sum(os.path.getsize(f) for f in Path(file_path).rglob('*') if f.is_file())
    return 0

def collect_files_to_process(repo, path, max_size):
    """Collect files to process, grouping small files together"""
    items_to_process = []
    total_size = 0
    queue = deque([path])
    
    while queue:
        current_path = queue.popleft()
        
        if os.path.isfile(current_path):
            file_size = get_file_size(current_path)
            
            if file_size > max_size:
                logging.warning(f"Skipping large file: {current_path} ({file_size/1024/1024:.2f}MB)")
                continue
                
            if total_size + file_size <= max_size:
                rel_path = os.path.relpath(current_path, repo.working_dir)
                try:
                    if not repo.git.check_ignore(rel_path):  # Check if file is ignored
                        items_to_process.append(rel_path)
                        total_size += file_size
                    else:
                        logging.info(f"Skipping ignored file: {rel_path}")
                except GitCommandError:
                    # If check-ignore fails, assume the file should be processed
                    items_to_process.append(rel_path)
                    total_size += file_size
                    logging.info(f"Git check-ignore failed for {rel_path}, including in commit")
            else:
                return items_to_process, total_size, current_path
        else:
            try:
                for entry in os.scandir(current_path):
                    if not entry.name.startswith('.'):
                        queue.append(entry.path)
            except PermissionError:
                logging.warning(f"Permission denied for: {current_path}")
    
    return items_to_process, total_size, None

def process_chunk(repo, chunk, commit_message, remote_name='origin'):
    """Add, commit, and push a chunk of files"""
    try:
        # Stage files
        repo.index.add(chunk)
        logging.info(f"Staged {len(chunk)} files")
        
        # Commit
        repo.index.commit(commit_message)
        logging.info(f"Committed: {commit_message}")
        
        # Push with retries
        for attempt in range(MAX_PUSH_RETRIES):
            try:
                repo.remote(name=remote_name).push()
                logging.info(f"Successfully pushed chunk: {commit_message}")
                return True
            except GitCommandError as e:
                if attempt < MAX_PUSH_RETRIES - 1:
                    logging.warning(f"Push attempt {attempt + 1} failed, retrying... Error: {e}")
                    continue
                raise
    except Exception as e:
        logging.error(f"Failed to process chunk: {str(e)}")
        return False

def process_repository(repo_path, remote_url):
    """Main processing function using gitpython"""
    try:
        repo = Repo(repo_path)
        
        # Setup remote if needed
        if not repo.remotes:
            repo.create_remote('origin', remote_url)
        
        # Get untracked and modified files
        untracked = repo.untracked_files
        changed = [item.a_path for item in repo.index.diff(None)]  # Unstaged changes
        
        all_files = set(untracked + changed)
        remaining_paths = [os.path.join(repo.working_dir, f) for f in all_files]
        
        while remaining_paths:
            current_path = remaining_paths.pop(0)
            
            items_to_process, chunk_size, remaining_path = collect_files_to_process(
                repo, current_path, MAX_CHUNK_SIZE
            )
            
            if remaining_path:
                remaining_paths.insert(0, remaining_path)
            
            if items_to_process:
                commit_msg = f"Added {len(items_to_process)} items (~{chunk_size/1024/1024:.2f}MB)"
                if not process_chunk(repo, items_to_process, commit_msg):
                    return False
                    
        return True
        
    except Exception as e:
        logging.error(f"Error processing repository: {str(e)}")
        return False

def main():
    setup_logging()
    
    repo_path = input("Enter directory path: ").strip()
    remote_url = input("Enter repo link: ").strip()
    
    if not os.path.isdir(repo_path):
        logging.error("Invalid directory path")
        return
    
    try:
        logging.info(f"Starting to process repository: {repo_path}")
        if process_repository(repo_path, remote_url):
            logging.info("Operation completed successfully")
        else:
            logging.error("Operation completed with errors")
            
    except Exception as e:
        logging.error(f"Operation failed: {str(e)}")

if __name__ == "__main__":
    main()