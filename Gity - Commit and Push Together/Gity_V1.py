import os
import subprocess
import logging
from pathlib import Path
from collections import deque

# Constants
MAX_CHUNK_SIZE = 25 * 1024 * 1024  # 25MB in bytes
GIT_IGNORE = ".gitignore"
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

def get_git_status(repo_path):
    """Get git status output in porcelain format"""
    try:
        result = subprocess.run(
            ['git', '-C', repo_path, 'status', '--porcelain'],
            capture_output=True, text=True, check=True
        )
        return result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to get git status: {e.stderr}")
        raise

def get_file_size(file_path):
    """Get size of a file or directory in bytes"""
    if os.path.isfile(file_path):
        return os.path.getsize(file_path)
    elif os.path.isdir(file_path):
        total_size = 0
        for dirpath, _, filenames in os.walk(file_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        return total_size
    return 0

def is_ignored(file_path, repo_path):
    """Check if a file is ignored by git"""
    try:
        result = subprocess.run(
            ['git', '-C', repo_path, 'check-ignore', file_path],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except subprocess.CalledProcessError:
        return False

def process_chunk(repo_path, chunk, commit_message, remote_url):
    """Add, commit, and push a chunk of files"""
    try:
        # Add files
        for file_path in chunk:
            subprocess.run(
                ['git', '-C', repo_path, 'add', file_path],
                check=True, capture_output=True
            )
            logging.info(f"Added: {file_path}")
        
        # Commit
        subprocess.run(
            ['git', '-C', repo_path, 'commit', '-m', commit_message],
            check=True, capture_output=True
        )
        logging.info(f"Committed: {commit_message}")
        
        # Push with retries
        for attempt in range(MAX_PUSH_RETRIES):
            try:
                subprocess.run(
                    ['git', '-C', repo_path, 'push', 'origin', 'HEAD'],
                    check=True, capture_output=True
                )
                logging.info(f"Successfully pushed chunk: {commit_message}")
                return True
            except subprocess.CalledProcessError as e:
                if attempt < MAX_PUSH_RETRIES - 1:
                    logging.warning(f"Push attempt {attempt + 1} failed, retrying... Error: {e.stderr}")
                    continue
                raise
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to process chunk: {e.stderr}")
        return False

def setup_remote(repo_path, remote_url):
    """Configure remote repository"""
    try:
        # Check if remote exists
        result = subprocess.run(
            ['git', '-C', repo_path, 'remote', '-v'],
            capture_output=True, text=True
        )
        
        if remote_url not in result.stdout:
            # Set remote
            subprocess.run(
                ['git', '-C', repo_path, 'remote', 'add', 'origin', remote_url],
                check=True, capture_output=True
            )
            logging.info("Configured remote repository")
        
        # Set default push behavior
        subprocess.run(
            ['git', '-C', repo_path, 'config', 'push.default', 'current'],
            check=True, capture_output=True
        )
        
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to setup remote: {e.stderr}")
        return False

def collect_files_to_process(repo_path, path, processed_files):
    """Collect files to process, grouping small files together"""
    items_to_process = []
    total_size = 0
    
    # Use a queue for BFS traversal
    queue = deque()
    queue.append(path)
    
    while queue:
        current_path = queue.popleft()
        
        if os.path.isfile(current_path):
            file_size = get_file_size(current_path)
            
            # If single file is too large, skip it
            if file_size > MAX_CHUNK_SIZE:
                logging.warning(f"Skipping large file: {current_path} ({file_size/1024/1024:.2f}MB)")
                continue
                
            # Add to current chunk if it fits
            if total_size + file_size <= MAX_CHUNK_SIZE:
                rel_path = os.path.relpath(current_path, repo_path)
                items_to_process.append(rel_path)
                total_size += file_size
            else:
                # Return the current chunk and remaining path
                return items_to_process, total_size, current_path
        else:
            # For directories, add contents to queue
            try:
                for entry in os.scandir(current_path):
                    if not entry.name.startswith('.') and not is_ignored(entry.path, repo_path):
                        queue.append(entry.path)
            except PermissionError:
                logging.warning(f"Permission denied for: {current_path}")
                continue
    
    return items_to_process, total_size, None

def process_repository(repo_path, remote_url):
    """Main function to process the repository"""
    try:
        # Setup remote first
        if not setup_remote(repo_path, remote_url):
            return False
        
        # Get git status
        status_lines = get_git_status(repo_path)
        
        # Process files
        remaining_paths = []
        
        for line in status_lines:
            status = line[:2].strip()
            file_path = line[3:]
            
            if is_ignored(file_path, repo_path):
                logging.info(f"Skipping ignored file: {file_path}")
                continue
            
            abs_path = os.path.join(repo_path, file_path)
            remaining_paths.append(abs_path)
        
        while remaining_paths:
            current_path = remaining_paths.pop(0)
            
            # Collect files to process in this chunk
            items_to_process, chunk_size, remaining_path = collect_files_to_process(
                repo_path, current_path, set()
            )
            
            if remaining_path:
                # Put the remaining path back in the queue
                remaining_paths.insert(0, remaining_path)
            
            if items_to_process:
                commit_message = f"Added {len(items_to_process)} items (~{chunk_size/1024/1024:.2f}MB)"
                if not process_chunk(repo_path, items_to_process, commit_message, remote_url):
                    return False
                    
        return True
            
    except Exception as e:
        logging.error(f"Error processing repository: {str(e)}")
        return False

def main():
    setup_logging()
    
    # Get user input
    repo_path = input("Enter directory path: ").strip()
    remote_url = input("Enter repo link: ").strip()
    
    # Validate paths
    if not os.path.isdir(repo_path):
        logging.error("Invalid directory path")
        return
    
    # Process repository
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