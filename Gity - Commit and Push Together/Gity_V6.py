import os
import time
import hashlib
import logging
from pathlib import Path
from collections import deque
from typing import List, Tuple, Optional, Set, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from git import Repo, GitCommandError
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text
from rich.style import Style
from rich.logging import RichHandler

# Constants
MAX_CHUNK_SIZE = 25 * 1024 * 1024  # 25MB
MAX_PUSH_RETRIES = 3
MIN_COMMIT_INTERVAL = 30  # seconds between commits
MAX_COMMIT_MESSAGE_LENGTH = 100
MAX_WORKERS = 4
LOG_DIR = "push_logs"

# Initialize Rich console
console = Console()

# Global caches
file_size_cache: Dict[str, int] = {}
fingerprint_cache: Dict[str, str] = {}

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

def get_file_size(file_path: str) -> int:
    """Get size of a file or directory with caching"""
    if file_path in file_size_cache:
        return file_size_cache[file_path]
    
    try:
        if os.path.isfile(file_path):
            size = os.path.getsize(file_path)
        elif os.path.isdir(file_path):
            size = sum(get_file_size(os.path.join(root, f)) 
                     for root, _, files in os.walk(file_path) 
                     for f in files)
        else:
            size = 0
    except (OSError, PermissionError) as e:
        logging.warning(f"Could not get size for {file_path}: {str(e)}")
        size = 0
    
    file_size_cache[file_path] = size
    return size

def generate_file_fingerprint(file_path: str) -> str:
    """Generate a fingerprint for file content with caching"""
    if file_path in fingerprint_cache:
        return fingerprint_cache[file_path]
    
    if not os.path.isfile(file_path):
        return ""
    
    hasher = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        fingerprint = hasher.hexdigest()
    except Exception:
        fingerprint = ""
    
    fingerprint_cache[file_path] = fingerprint
    return fingerprint

def setup_logging():
    """Setup comprehensive logging with timestamped files"""
    log_filename = f"git_pusher_{time.strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = os.path.join(LOG_DIR, log_filename)
    
    file_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        tracebacks_show_locals=True,
        markup=True
    )
    console_handler.setLevel(logging.INFO)
    
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)
    
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[console_handler, file_handler]
    )
    logging.getLogger("git").setLevel(logging.WARNING)

def log_operation_summary(operation: str, details: dict):
    """Log detailed operation summaries"""
    logging.info(f"OPERATION: {operation.upper()}")
    for key, value in details.items():
        logging.info(f"  {key:<20}: {value}")
    logging.info("-" * 60)

def log_chunk_details(chunk: List[str], chunk_size: int, commit_msg: str):
    """Log detailed information about a chunk being processed"""
    logging.debug(f"CHUNK DETAILS | Files: {len(chunk)} | Size: {chunk_size/1024/1024:.2f}MB")
    for i, file in enumerate(chunk, 1):
        logging.debug(f"  {i:>3}. {file}")
    logging.debug(f"COMMIT MSG: {commit_msg}")

def log_push_attempt(attempt: int, success: bool, message: str = ""):
    """Log push attempts with results"""
    status = "SUCCESS" if success else "FAILED"
    logging.info(f"PUSH ATTEMPT {attempt}/{MAX_PUSH_RETRIES}: {status}")
    if message:
        logging.info(f"  MESSAGE: {message}")

def log_error(error: Exception, context: str = ""):
    """Standardized error logging"""
    logging.error(f"ERROR in {context}: {str(error)}", exc_info=True)
    logging.error("-" * 60)

def display_header():
    """Display beautiful header with version info"""
    header_text = Text("✨ Git Repository Pusher V6 ✨", style="bold blue on black")
    header = Panel(
        header_text,
        style="bright_blue",
        border_style="yellow",
        expand=False
    )
    console.print(header)

    info_table = Table.grid(padding=1)
    info_table.add_column(style="bold cyan")
    info_table.add_column(style="green")
    
    info_table.add_row("Version:", "6.0 (Enhanced Logging)")
    info_table.add_row("Log Directory:", LOG_DIR)
    info_table.add_row("Max Chunk Size:", f"{MAX_CHUNK_SIZE/1024/1024:.0f}MB")
    
    console.print(Panel(info_table, title="ℹ️ Information", border_style="green"))

def validate_inputs(repo_path: str, remote_url: str) -> bool:
    """Validate user inputs with better error messages"""
    if not os.path.isdir(repo_path):
        console.print(
            Panel.fit(
                f"[bold red]✗ Invalid directory path: {repo_path}[/]",
                border_style="red"
            )
        )
        return False
    
    if not remote_url.startswith(('http://', 'https://', 'git@')):
        console.print(
            Panel.fit(
                "[bold red]✗ Remote URL should be a valid git repository URL[/]\n"
                f"[dim]Examples:\n"
                f"https://github.com/user/repo.git\n"
                f"git@github.com:user/repo.git[/]\n"
                f"[red]Provided URL: {remote_url}[/]",
                border_style="red"
            )
        )
        return False
    
    return True

def parallel_check_ignore(repo: Repo, files: List[str]) -> Set[str]:
    """Check ignore status in parallel"""
    ignored = set()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(repo.git.check_ignore, f): f for f in files}
        for future in as_completed(futures):
            file = futures[future]
            try:
                if future.result():
                    ignored.add(file)
            except GitCommandError:
                pass
    return ignored

def collect_files_to_process(repo: Repo, paths: List[str], max_size: int) -> Tuple[List[str], int, List[str]]:
    """Collect files to process with parallel processing"""
    items_to_process = []
    total_size = 0
    remaining_paths = []
    
    # First pass: quick scan of all files
    all_files = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task("Scanning files...", total=len(paths))
        
        for path in paths:
            if os.path.isfile(path):
                all_files.append(path)
            elif os.path.isdir(path):
                for root, _, files in os.walk(path):
                    all_files.extend(os.path.join(root, f) for f in files)
            progress.update(task, advance=1)
    
    # Filter ignored files in parallel
    rel_paths = [os.path.relpath(f, repo.working_dir) for f in all_files]
    ignored = parallel_check_ignore(repo, rel_paths)
    
    # Process files in order of size (smallest first)
    files_to_check = sorted(
        [f for f in all_files if os.path.relpath(f, repo.working_dir) not in ignored],
        key=get_file_size
    )
    
    for file_path in files_to_check:
        rel_path = os.path.relpath(file_path, repo.working_dir)
        file_size = get_file_size(file_path)
        
        if file_size > max_size:
            logging.warning(f"Skipping large file: {rel_path} ({file_size/1024/1024:.2f}MB)")
            continue
            
        if total_size + file_size <= max_size:
            items_to_process.append(rel_path)
            total_size += file_size
        else:
            remaining_paths.append(file_path)
    
    return items_to_process, total_size, remaining_paths

def process_chunk(repo: Repo, chunk: List[str], commit_message: str, progress: Progress, task_id: int) -> bool:
    """Process a chunk with detailed logging"""
    chunk_size = sum(get_file_size(os.path.join(repo.working_dir, f)) for f in chunk)
    
    # Log chunk details before processing
    log_operation_summary("processing_chunk", {
        "file_count": len(chunk),
        "total_size": f"{chunk_size/1024/1024:.2f}MB",
        "first_file": chunk[0] if chunk else "N/A",
        "commit_message": commit_message
    })
    log_chunk_details(chunk, chunk_size, commit_message)
    
    try:
        # Stage files
        with progress:
            progress.update(task_id, description="Staging files...")
            for file in chunk:
                repo.index.add(file)
                logging.debug(f"Staged: {file}")
                progress.advance(task_id)
        
        # Commit
        if len(commit_message) > MAX_COMMIT_MESSAGE_LENGTH:
            original_msg = commit_message
            commit_message = commit_message[:MAX_COMMIT_MESSAGE_LENGTH-3] + "..."
            logging.warning(f"Truncated commit message from {len(original_msg)} to {len(commit_message)} chars")
        
        commit = repo.index.commit(commit_message)
        logging.info(f"COMMIT CREATED: {commit.hexsha[:7]} - {commit_message}")
        
        # Rate limiting
        with progress:
            wait_msg = f"Waiting {MIN_COMMIT_INTERVAL}s between commits..."
            progress.update(task_id, description=wait_msg)
            logging.info(wait_msg)
            time.sleep(MIN_COMMIT_INTERVAL)
        
        # Push with retries
        for attempt in range(1, MAX_PUSH_RETRIES + 1):
            try:
                logging.info(f"PUSH ATTEMPT {attempt} STARTED")
                push_result = repo.remote(name='origin').push()
                
                if any('rejected' in str(info.flags) for info in push_result):
                    error_msg = "Push was rejected by remote"
                    raise GitCommandError('push', error_msg)
                
                log_push_attempt(attempt, True, "Push successful")
                return True
                
            except GitCommandError as e:
                log_push_attempt(attempt, False, str(e))
                if attempt < MAX_PUSH_RETRIES:
                    wait_time = (2 ** attempt) * 5
                    logging.warning(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise
                    
    except Exception as e:
        log_error(e, "process_chunk")
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
    """Main processing function with comprehensive logging"""
    log_operation_summary("repository_processing_start", {
        "repository_path": repo_path,
        "remote_url": remote_url,
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    })
    
    try:
        repo = Repo(repo_path)
        
        if repo.bare:
            logging.error("Repository is bare (no working directory)")
            return False
            
        if repo.is_dirty():
            logging.warning("Repository has uncommitted changes (will not be processed)")
        
        # Setup remote
        if not repo.remotes:
            logging.info("Creating new remote 'origin'")
            repo.create_remote('origin', remote_url)
        
        # Check remote connection
        if not check_remote_connection(repo):
            return False
        
        # Get files to process
        untracked = repo.untracked_files
        changed = [item.a_path for item in repo.index.diff(None)]
        staged = [item.a_path for item in repo.index.diff('HEAD')]
        
        all_files = set(untracked + changed + staged)
        total_files = len(all_files)
        logging.info(f"TOTAL FILES TO PROCESS: {total_files}")
        
        if not total_files:
            logging.warning("No files to process - working directory clean")
            return True
        
        remaining_paths = [os.path.join(repo.working_dir, f) for f in all_files]
        processed_files = 0
        start_time = time.time()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=10,
        ) as progress:
            main_task = progress.add_task(
                f"Processing {total_files} files",
                total=total_files
            )
            
            while remaining_paths:
                chunk, chunk_size, remaining_paths = collect_files_to_process(
                    repo, remaining_paths, MAX_CHUNK_SIZE
                )
                
                if not chunk:
                    break
                    
                commit_msg = (
                    f"Added {len(chunk)} items (~{chunk_size/1024/1024:.2f}MB) "
                    f"[{processed_files + len(chunk)}/{total_files}]"
                )
                
                chunk_task = progress.add_task(
                    f"Processing chunk",
                    total=len(chunk)
                )
                
                success = process_chunk(repo, chunk, commit_msg, progress, chunk_task)
                progress.remove_task(chunk_task)
                
                if not success:
                    return False
                    
                processed_files += len(chunk)
                progress.update(main_task, completed=processed_files)
                
                # Update progress file
                with open(os.path.join(LOG_DIR, "push_progress.txt"), "w") as f:
                    f.write(f"{processed_files}/{total_files} files processed\n")
                    f.write(f"Last update: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    elapsed = time.time() - start_time
                    if processed_files > 0:
                        remaining = (elapsed / processed_files) * (total_files - processed_files)
                        f.write(f"Estimated remaining time: {remaining:.1f} seconds\n")
        
        return True
        
    except Exception as e:
        log_error(e, "process_repository")
        return False

def main():
    """Main function with enhanced logging"""
    setup_logging()
    display_header()
    
    repo_path = console.input("[bold cyan]📁 Enter directory path: [/]").strip()
    remote_url = console.input("[bold cyan]🔗 Enter repo URL: [/]").strip()
    
    if not validate_inputs(repo_path, remote_url):
        return
    
    try:
        console.print()
        console.rule("[bold blue]Starting Processing[/]", align="left")
        logging.info("Script started")
        
        start_time = time.time()
        success = process_repository(repo_path, remote_url)
        
        console.rule("[bold blue]Processing Complete[/]", align="left")
        elapsed_time = time.time() - start_time
        
        if success:
            console.print(
                Panel.fit(
                    f"[bold green]✓ Operation completed successfully![/]\n"
                    f"[dim]Time elapsed: {elapsed_time:.1f} seconds[/]",
                    border_style="green"
                )
            )
            logging.info(f"Operation completed in {elapsed_time:.1f} seconds")
        else:
            console.print(
                Panel.fit(
                    "[bold yellow]⚠️ Operation completed with errors[/]",
                    border_style="yellow"
                )
            )
            logging.warning("Operation completed with errors")
            
    except KeyboardInterrupt:
        console.print(
            Panel.fit(
                "[bold yellow]⚠️ Operation cancelled by user[/]",
                border_style="yellow"
            )
        )
        logging.warning("Operation cancelled by user")
    except Exception as e:
        console.print(
            Panel.fit(
                f"[bold red]✗ Operation failed[/]\n"
                f"[red]Error:[/] {str(e)}",
                border_style="red"
            )
        )
        log_error(e, "main")

if __name__ == "__main__":
    main()