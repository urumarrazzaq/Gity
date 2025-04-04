import os
import time
import hashlib
import logging
from pathlib import Path
from collections import deque
from typing import List, Tuple, Optional, Set
from git import Repo, GitCommandError
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text
from rich.style import Style
from rich.logging import RichHandler

# Constants
MAX_CHUNK_SIZE = 25 * 1024 * 1024  # 25MB
MAX_PUSH_RETRIES = 3
MIN_COMMIT_INTERVAL = 30  # seconds between commits
MAX_COMMIT_MESSAGE_LENGTH = 100

# Initialize Rich console
console = Console()

def setup_logging():
    """Setup rich logging with beautiful formatting"""
    from rich.logging import RichHandler

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=console,
                rich_tracebacks=True,
                tracebacks_show_locals=True,
                markup=True
            )
        ]
    )
    logging.getLogger("git").setLevel(logging.WARNING)

def display_header():
    """Display beautiful header"""
    header_text = Text("✨ Git Repository Pusher ✨", style="bold blue on black")
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
    
    info_table.add_row("Version:", "4.0 (Rich Enhanced)")
    info_table.add_row("Features:", "Beautiful terminal interface, smart chunking")
    info_table.add_row("Max Chunk Size:", f"{MAX_CHUNK_SIZE/1024/1024:.0f}MB")
    
    console.print(Panel(info_table, title="ℹ️ Information", border_style="green"))

def validate_inputs(repo_path: str, remote_url: str) -> bool:
    """Validate user inputs before processing"""
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
                f"[dim]Provided URL: {remote_url}[/]",
                border_style="red"
            )
        )
        return False
    
    return True

def get_file_size(file_path: str) -> int:
    """Get size of a file or directory in bytes with rich progress"""
    try:
        if os.path.isfile(file_path):
            return os.path.getsize(file_path)
        elif os.path.isdir(file_path):
            total_size = 0
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True,
            ) as progress:
                task = progress.add_task(
                    f"Calculating size for {os.path.basename(file_path)}...", 
                    total=None
                )
                for f in Path(file_path).rglob('*'):
                    if f.is_file():
                        total_size += os.path.getsize(f)
            return total_size
    except (OSError, PermissionError) as e:
        logging.warning(f"[yellow]Could not get size for {file_path}: {str(e)}[/]")
    return 0

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
    """Collect files to process with rich progress display"""
    items_to_process = []
    total_size = 0
    queue = deque([path])
    processed_files: Set[str] = set()
    
    with console.status("[bold green]Scanning files...") as status:
        while queue:
            current_path = queue.popleft()
            
            if os.path.isfile(current_path):
                if current_path in processed_files:
                    continue
                    
                processed_files.add(current_path)
                file_size = get_file_size(current_path)
                
                if file_size > max_size:
                    logging.warning(f"[yellow]Skipping large file: {current_path} ({file_size/1024/1024:.2f}MB)[/]")
                    continue
                    
                rel_path = os.path.relpath(current_path, repo.working_dir)
                
                try:
                    if repo.git.check_ignore(rel_path):
                        logging.info(f"[dim]Skipping ignored file: {rel_path}[/]")
                        continue
                except GitCommandError:
                    logging.info(f"[dim]Git check-ignore failed for {rel_path}, including in commit[/]")
                
                if rel_path in [item.a_path for item in repo.index.diff(None)]:
                    file_fingerprint = generate_file_fingerprint(current_path)
                    try:
                        old_content = repo.git.show(f"HEAD:{rel_path}")
                        old_fingerprint = hashlib.sha256(old_content.encode()).hexdigest()
                        if file_fingerprint == old_fingerprint:
                            logging.info(f"[dim]Skipping unchanged file: {rel_path}[/]")
                            continue
                    except GitCommandError:
                        pass
                
                if total_size + file_size <= max_size:
                    items_to_process.append(rel_path)
                    total_size += file_size
                    status.update(f"Collected {len(items_to_process)} files (~{total_size/1024/1024:.2f}MB)")
                else:
                    return items_to_process, total_size, current_path
            else:
                try:
                    dir_entries = []
                    for entry in os.scandir(current_path):
                        if not entry.name.startswith('.'):
                            dir_entries.append(entry)
                    
                    dir_entries.sort(key=lambda e: (
                        not e.is_file(),
                        not e.name.endswith(('.cpp', '.h', '.py', '.txt')),
                        get_file_size(e.path) if e.is_file() else 0
                    ))
                    
                    for entry in dir_entries:
                        queue.appendleft(entry.path)
                except PermissionError:
                    logging.warning(f"[yellow]Permission denied for: {current_path}[/]")
    
    return items_to_process, total_size, None

def process_chunk(repo: Repo, chunk: List[str], commit_message: str, remote_name: str = 'origin') -> bool:
    """Process a chunk with beautiful progress display"""
    try:
        # Create a table for the commit summary
        commit_table = Table(
            title="🚀 Processing Commit",
            show_header=True,
            header_style="bold magenta",
            border_style="blue"
        )
        commit_table.add_column("File", style="cyan")
        commit_table.add_column("Status", style="green")
        
        # Stage files with progress
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Staging files...", total=len(chunk))
            for file in chunk:
                repo.index.add(file)
                commit_table.add_row(file, "✅ Staged")
                progress.update(task, advance=1)
        
        # Truncate commit message if needed
        if len(commit_message) > MAX_COMMIT_MESSAGE_LENGTH:
            commit_message = commit_message[:MAX_COMMIT_MESSAGE_LENGTH-3] + "..."
        
        # Commit
        repo.index.commit(commit_message)
        commit_table.add_row("Commit", f"✅ {commit_message}")
        
        # Display the commit summary
        console.print(commit_table)
        
        # Rate limiting
        with console.status(
            f"[bold yellow]Waiting {MIN_COMMIT_INTERVAL}s between commits...",
            spinner="clock"
        ):
            time.sleep(MIN_COMMIT_INTERVAL)
        
        # Push with retries
        for attempt in range(MAX_PUSH_RETRIES):
            try:
                push_result = repo.remote(name=remote_name).push()
                
                if any('rejected' in str(info.flags) for info in push_result):
                    raise GitCommandError('push', "Push was rejected by remote")
                
                console.print(
                    Panel.fit(
                        f"[bold green]✓ Successfully pushed chunk![/]\n"
                        f"[dim]Commit:[/] {commit_message}",
                        border_style="green"
                    )
                )
                return True
            except GitCommandError as e:
                if attempt < MAX_PUSH_RETRIES - 1:
                    wait_time = (2 ** attempt) * 5
                    console.print(
                        Panel.fit(
                            f"[yellow]⚠️ Push attempt {attempt + 1} failed[/]\n"
                            f"[dim]Retrying in {wait_time}s...[/]\n"
                            f"[red]Error:[/] {e}",
                            border_style="yellow"
                        )
                    )
                    time.sleep(wait_time)
                    continue
                console.print(
                    Panel.fit(
                        f"[bold red]✗ Push failed after {MAX_PUSH_RETRIES} attempts[/]\n"
                        f"[red]Error:[/] {e}",
                        border_style="red"
                    )
                )
                raise
    except Exception as e:
        console.print(
            Panel.fit(
                f"[bold red]✗ Failed to process chunk[/]\n"
                f"[red]Error:[/] {str(e)}",
                border_style="red"
            )
        )
        return False

def check_remote_connection(repo: Repo, remote_name: str = 'origin') -> bool:
    """Check if we can connect to the remote repository"""
    try:
        remote = repo.remote(name=remote_name)
        remote.fetch()
        return True
    except Exception as e:
        logging.error(f"[red]Failed to connect to remote: {str(e)}[/]")
        return False

def process_repository(repo_path: str, remote_url: str) -> bool:
    """Main processing function with enhanced error handling and progress tracking"""
    try:
        repo = Repo(repo_path)
        
        # Verify repository state
        if repo.bare:
            console.print(
                Panel.fit(
                    "[bold red]✗ Repository is bare (no working directory)[/]",
                    border_style="red"
                )
            )
            return False
            
        if repo.is_dirty():
            console.print(
                Panel.fit(
                    "[yellow]⚠️ Repository has uncommitted changes that will not be processed[/]",
                    border_style="yellow"
                )
            )
        
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
        console.print(
            Panel.fit(
                f"[bold red]✗ Error processing repository[/]\n"
                f"[red]Error:[/] {str(e)}",
                border_style="red"
            )
        )
        return False

def main():
    """Main function with beautiful interface"""
    setup_logging()
    display_header()
    
    # Get user input with style
    repo_path = console.input("[bold cyan]📁 Enter directory path: [/]").strip()
    remote_url = console.input("[bold cyan]🔗 Enter repo link: [/]").strip()
    
    if not validate_inputs(repo_path, remote_url):
        return
    
    try:
        console.print()
        console.rule("[bold blue]Starting Processing[/]", align="left")
        
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
        else:
            console.print(
                Panel.fit(
                    "[bold yellow]⚠️ Operation completed with errors[/]",
                    border_style="yellow"
                )
            )
            
    except KeyboardInterrupt:
        console.print(
            Panel.fit(
                "[bold yellow]⚠️ Operation cancelled by user[/]",
                border_style="yellow"
            )
        )
    except Exception as e:
        console.print(
            Panel.fit(
                f"[bold red]✗ Operation failed[/]\n"
                f"[red]Error:[/] {str(e)}",
                border_style="red"
            )
        )

if __name__ == "__main__":
    main()