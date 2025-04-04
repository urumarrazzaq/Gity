import os
import subprocess
import logging
from pathlib import Path

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

def process_repository(repo_path, remote_url):
    """Main function to process the repository"""
    try:
        # Setup remote first
        if not setup_remote(repo_path, remote_url):
            return False
        
        # Get git status
        status_lines = get_git_status(repo_path)
        
        # Process files
        current_chunk = []
        current_size = 0
        
        for line in status_lines:
            # Parse porcelain status line
            status = line[:2].strip()
            file_path = line[3:]
            
            # Skip ignored files
            if is_ignored(file_path, repo_path):
                logging.info(f"Skipping ignored file: {file_path}")
                continue
            
            # Get file size
            abs_path = os.path.join(repo_path, file_path)
            size = get_file_size(abs_path)
            
            # Handle large files
            if size > MAX_CHUNK_SIZE:
                if os.path.isfile(abs_path):
                    logging.warning(f"Skipping large file: {file_path} ({size/1024/1024:.2f}MB)")
                    continue
                else:
                    # Process large directory recursively
                    logging.info(f"Processing large directory recursively: {file_path}")
                    process_directory(abs_path, repo_path, remote_url)
                    continue
            
            # Add to current chunk if it fits
            if current_size + size <= MAX_CHUNK_SIZE:
                current_chunk.append(file_path)
                current_size += size
            else:
                # Commit and push current chunk
                if current_chunk:
                    commit_message = f"Added {', '.join([Path(f).name for f in current_chunk])}"
                    if not process_chunk(repo_path, current_chunk, commit_message, remote_url):
                        return False
                    current_chunk = [file_path]
                    current_size = size
        
        # Commit and push remaining files
        if current_chunk:
            commit_message = f"Added {', '.join([Path(f).name for f in current_chunk])}"
            if not process_chunk(repo_path, current_chunk, commit_message, remote_url):
                return False
                
        return True
            
    except Exception as e:
        logging.error(f"Error processing repository: {str(e)}")
        return False

def process_directory(dir_path, repo_path, remote_url):
    """Process a directory recursively to find manageable chunks"""
    try:
        for entry in os.scandir(dir_path):
            if entry.name.startswith('.') or is_ignored(entry.path, repo_path):
                continue
                
            size = get_file_size(entry.path)
            
            if size > MAX_CHUNK_SIZE:
                if entry.is_file():
                    logging.warning(f"Skipping large file: {entry.path} ({size/1024/1024:.2f}MB)")
                    continue
                else:
                    process_directory(entry.path, repo_path, remote_url)
            else:
                rel_path = os.path.relpath(entry.path, repo_path)
                commit_message = f"Added {entry.name}"
                if not process_chunk(repo_path, [rel_path], commit_message, remote_url):
                    return False
                    
        return True
    except Exception as e:
        logging.error(f"Error processing directory {dir_path}: {str(e)}")
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