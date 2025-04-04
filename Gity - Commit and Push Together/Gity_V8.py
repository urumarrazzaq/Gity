import os
import time
import hashlib
import logging
import json
import platform
from pathlib import Path
from collections import deque, defaultdict
from typing import List, Tuple, Optional, Set, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from git import Repo, GitCommandError, Git
from datetime import datetime

# PyQt5 imports for UI
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTreeWidget, QTreeWidgetItem, 
                            QVBoxLayout, QWidget, QLabel, QPushButton, QLineEdit, 
                            QProgressBar, QHBoxLayout, QMessageBox, QFileDialog, 
                            QSplitter, QTextEdit, QStatusBar, QComboBox, QTabWidget,
                            QGroupBox, QFormLayout, QCheckBox, QScrollArea, QSizePolicy)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QSettings, QTimer
from PyQt5.QtGui import QIcon, QColor, QFont, QPalette, QTextCursor, QSyntaxHighlighter, QTextCharFormat

# Constants
MAX_CHUNK_SIZE = 25 * 1024 * 1024  # 25MB
MAX_PUSH_RETRIES = 3
MIN_COMMIT_INTERVAL = 2  # seconds between commits
MAX_COMMIT_MESSAGE_LENGTH = 100
MAX_WORKERS = 4
LOG_DIR = "push_logs"
STATE_FILE = "push_state.json"
CONFIG_FILE = "git_pusher_config.ini"

# Color constants for UI
COLOR_UNTRACKED = QColor(255, 165, 0)  # Orange
COLOR_MODIFIED = QColor(255, 255, 0)    # Yellow
COLOR_STAGED = QColor(100, 149, 237)    # Cornflower blue
COLOR_PUSHED = QColor(50, 205, 50)      # Lime green
COLOR_IGNORED = QColor(169, 169, 169)   # Dark gray
COLOR_ERROR = QColor(255, 0, 0)         # Red
COLOR_DEFAULT = QColor(255, 255, 255)   # White
COLOR_CONFLICT = QColor(255, 0, 255)    # Magenta

class StateManager:
    """Handles saving and loading application state"""
    def __init__(self):
        self.state_file = Path(STATE_FILE)
        
    def save_state(self, data: dict):
        """Save the current state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logging.error(f"Error saving state: {str(e)}")
            return False
            
    def load_state(self) -> dict:
        """Load state from file if exists"""
        if not self.state_file.exists():
            return {}
            
        try:
            with open(self.state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading state: {str(e)}")
            return {}
            
    def clear_state(self):
        """Clear the saved state"""
        try:
            if self.state_file.exists():
                self.state_file.unlink()
            return True
        except Exception as e:
            logging.error(f"Error clearing state: {str(e)}")
            return False

class GitPusherThread(QThread):
    """Thread for running the git push operations with enhanced features"""
    progress_signal = pyqtSignal(int, int, str)  # current, total, message
    file_status_signal = pyqtSignal(str, str)    # file_path, status
    operation_complete = pyqtSignal(bool, str)   # success, message
    log_signal = pyqtSignal(str, str)           # message, level
    branch_info_signal = pyqtSignal(list, str)  # branches, current_branch
    stats_signal = pyqtSignal(dict)             # repository statistics
    conflict_detected = pyqtSignal(str)         # file with conflict
    
    def __init__(self, repo_path, remote_url, branch_name=None, resume=False):
        super().__init__()
        self.repo_path = repo_path
        self.remote_url = remote_url
        self.branch_name = branch_name
        self.resume = resume
        self._is_running = True
        self.state = StateManager()
        
        # Initialize caches
        self.file_size_cache: Dict[str, int] = {}
        self.fingerprint_cache: Dict[str, str] = {}
        
        # Ensure log directory exists
        os.makedirs(LOG_DIR, exist_ok=True)
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging that emits to UI"""
        log_filename = f"git_pusher_{time.strftime('%Y%m%d_%H%M%S')}.log"
        self.log_filepath = os.path.join(LOG_DIR, log_filename)
        
        self.logger = logging.getLogger("GitPusher")
        self.logger.setLevel(logging.DEBUG)
        
        # File handler
        file_handler = logging.FileHandler(self.log_filepath)
        file_handler.setFormatter(logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        self.logger.addHandler(file_handler)
        
        # UI handler
        class UIHandler(logging.Handler):
            def __init__(self, signal):
                super().__init__()
                self.signal = signal
            
            def emit(self, record):
                msg = self.format(record)
                self.signal.emit(msg, record.levelname.lower())
        
        ui_handler = UIHandler(self.log_signal)
        ui_handler.setLevel(logging.INFO)
        self.logger.addHandler(ui_handler)
        
        logging.getLogger("git").setLevel(logging.WARNING)
    
    def log_operation_summary(self, operation: str, details: dict):
        """Log operation summaries"""
        self.logger.info(f"OPERATION: {operation.upper()}")
        for key, value in details.items():
            self.logger.info(f"  {key:<20}: {value}")
        self.logger.info("-" * 60)
    
    def get_file_size(self, file_path: str) -> int:
        """Get size of a file or directory with caching"""
        if file_path in self.file_size_cache:
            return self.file_size_cache[file_path]
        
        try:
            if os.path.isfile(file_path):
                size = os.path.getsize(file_path)
            elif os.path.isdir(file_path):
                size = sum(self.get_file_size(os.path.join(root, f)) 
                         for root, _, files in os.walk(file_path) 
                         for f in files)
            else:
                size = 0
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Could not get size for {file_path}: {str(e)}")
            size = 0
        
        self.file_size_cache[file_path] = size
        return size
    
    def generate_file_fingerprint(self, file_path: str) -> str:
        """Generate a fingerprint for file content with caching"""
        if file_path in self.fingerprint_cache:
            return self.fingerprint_cache[file_path]
        
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
        
        self.fingerprint_cache[file_path] = fingerprint
        return fingerprint
    
    def parallel_check_ignore(self, repo: Repo, files: List[str]) -> Set[str]:
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
    
    def collect_files_to_process(self, repo: Repo, paths: List[str], max_size: int) -> Tuple[List[str], int, List[str]]:
        """Collect files to process with parallel processing"""
        items_to_process = []
        total_size = 0
        remaining_paths = []
        
        # First pass: quick scan of all files
        all_files = []
        for path in paths:
            if os.path.isfile(path):
                all_files.append(path)
            elif os.path.isdir(path):
                for root, _, files in os.walk(path):
                    all_files.extend(os.path.join(root, f) for f in files)
        
        # Filter ignored files in parallel
        rel_paths = [os.path.relpath(f, repo.working_dir) for f in all_files]
        ignored = self.parallel_check_ignore(repo, rel_paths)
        
        # Process files in order of size (smallest first)
        files_to_check = sorted(
            [f for f in all_files if os.path.relpath(f, repo.working_dir) not in ignored],
            key=self.get_file_size
        )
        
        for file_path in files_to_check:
            rel_path = os.path.relpath(file_path, repo.working_dir)
            file_size = self.get_file_size(file_path)
            
            if file_size > max_size:
                self.logger.warning(f"Skipping large file: {rel_path} ({file_size/1024/1024:.2f}MB)")
                self.file_status_signal.emit(rel_path, "ignored")
                continue
                
            if total_size + file_size <= max_size:
                items_to_process.append(rel_path)
                total_size += file_size
            else:
                remaining_paths.append(file_path)
        
        return items_to_process, total_size, remaining_paths
    
    def check_for_conflicts(self, repo: Repo, file_path: str) -> bool:
        """Check if a file has merge conflicts"""
        try:
            # Check if file has conflict markers
            with open(os.path.join(repo.working_dir, file_path), 'r') as f:
                content = f.read()
                if any(marker in content for marker in ('<<<<<<<', '=======', '>>>>>>>')):
                    self.logger.warning(f"Conflict detected in file: {file_path}")
                    self.conflict_detected.emit(file_path)
                    return True
        except Exception:
            pass
        return False
    
    def process_chunk(self, repo: Repo, chunk: List[str], commit_message: str) -> bool:
        """Process a chunk with detailed logging and conflict checking"""
        chunk_size = sum(self.get_file_size(os.path.join(repo.working_dir, f)) for f in chunk)
        
        self.log_operation_summary("processing_chunk", {
            "file_count": len(chunk),
            "total_size": f"{chunk_size/1024/1024:.2f}MB",
            "first_file": chunk[0] if chunk else "N/A",
            "commit_message": commit_message
        })
        
        try:
            # Check for conflicts first
            for file in chunk:
                if self.check_for_conflicts(repo, file):
                    return False
            
            # Stage files
            for file in chunk:
                repo.index.add(file)
                self.logger.debug(f"Staged: {file}")
                self.file_status_signal.emit(file, "staged")
                self.progress_signal.emit(1, 1, f"Staging {file}")
            
            # Commit
            if len(commit_message) > MAX_COMMIT_MESSAGE_LENGTH:
                original_msg = commit_message
                commit_message = commit_message[:MAX_COMMIT_MESSAGE_LENGTH-3] + "..."
                self.logger.warning(f"Truncated commit message from {len(original_msg)} to {len(commit_message)} chars")
            
            commit = repo.index.commit(commit_message)
            self.logger.info(f"COMMIT CREATED: {commit.hexsha[:7]} - {commit_message}")
            
            # Rate limiting
            wait_msg = f"Waiting {MIN_COMMIT_INTERVAL}s between commits..."
            self.logger.info(wait_msg)
            self.progress_signal.emit(0, 1, wait_msg)
            
            for i in range(MIN_COMMIT_INTERVAL):
                if not self._is_running:
                    return False
                time.sleep(1)
                self.progress_signal.emit(i, MIN_COMMIT_INTERVAL, f"Waiting... {MIN_COMMIT_INTERVAL - i}s left")
            
            # Push with retries
            for attempt in range(1, MAX_PUSH_RETRIES + 1):
                try:
                    self.logger.info(f"PUSH ATTEMPT {attempt} STARTED")
                    self.progress_signal.emit(0, 1, f"Pushing attempt {attempt}/{MAX_PUSH_RETRIES}")
                    
                    push_result = repo.remote(name='origin').push()
                    
                    if any('rejected' in str(info.flags) for info in push_result):
                        error_msg = "Push was rejected by remote"
                        raise GitCommandError('push', error_msg)
                    
                    self.logger.info(f"PUSH ATTEMPT {attempt}/{MAX_PUSH_RETRIES}: SUCCESS")
                    
                    # Update file statuses to pushed
                    for file in chunk:
                        self.file_status_signal.emit(file, "pushed")
                    
                    # Save successful state
                    state_data = {
                        'last_successful_commit': commit.hexsha,
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                        'processed_files': chunk
                    }
                    self.state.save_state(state_data)
                    
                    return True
                    
                except GitCommandError as e:
                    self.logger.warning(f"PUSH ATTEMPT {attempt}/{MAX_PUSH_RETRIES}: FAILED - {str(e)}")
                    if attempt < MAX_PUSH_RETRIES:
                        wait_time = (2 ** attempt) * 5
                        self.logger.warning(f"Retrying in {wait_time}s...")
                        for i in range(wait_time):
                            if not self._is_running:
                                return False
                            time.sleep(1)
                            self.progress_signal.emit(i, wait_time, f"Retrying in {wait_time - i}s...")
                    else:
                        raise
                        
        except Exception as e:
            self.logger.error(f"Error in process_chunk: {str(e)}", exc_info=True)
            
            # Update file statuses to error
            for file in chunk:
                self.file_status_signal.emit(file, "error")
                
            return False
    
    def check_remote_connection(self, repo: Repo, remote_name: str = 'origin') -> bool:
        """Check if we can connect to the remote repository"""
        try:
            remote = repo.remote(name=remote_name)
            remote.fetch()
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to remote: {str(e)}")
            return False
    
    def get_repo_stats(self, repo: Repo) -> dict:
        """Collect repository statistics"""
        stats = {
            'total_files': 0,
            'total_size': 0,
            'file_types': defaultdict(int),
            'last_commit': None,
            'branch_count': 0,
            'remote_count': 0,
            'untracked_files': 0,
            'modified_files': 0,
            'staged_files': 0
        }
        
        try:
            # File statistics
            for root, _, files in os.walk(repo.working_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        size = os.path.getsize(file_path)
                        stats['total_files'] += 1
                        stats['total_size'] += size
                        
                        # Count by file extension
                        ext = os.path.splitext(file)[1].lower()
                        if ext:
                            stats['file_types'][ext] += 1
                    except OSError:
                        pass
            
            # Git statistics
            stats['last_commit'] = str(repo.head.commit.authored_datetime) if not repo.bare else None
            stats['branch_count'] = len(list(repo.branches))
            stats['remote_count'] = len(repo.remotes)
            
            # Status statistics
            stats['untracked_files'] = len(repo.untracked_files)
            stats['modified_files'] = len([item.a_path for item in repo.index.diff(None)])
            stats['staged_files'] = len([item.a_path for item in repo.index.diff('HEAD')])
            
            # Convert defaultdict to regular dict for JSON serialization
            stats['file_types'] = dict(stats['file_types'])
            
        except Exception as e:
            self.logger.error(f"Error collecting repo stats: {str(e)}")
        
        return stats
    
    def get_branch_info(self, repo: Repo) -> Tuple[List[str], str]:
        """Get list of branches and current branch"""
        branches = [str(branch) for branch in repo.branches]
        current_branch = repo.active_branch.name if not repo.bare and not repo.head.is_detached else "DETACHED"
        return branches, current_branch
    
    def stop(self):
        """Stop the thread gracefully"""
        self._is_running = False
    
    def run(self):
        """Main processing function with resume capability"""
        self._is_running = True
        
        self.log_operation_summary("repository_processing_start", {
            "repository_path": self.repo_path,
            "remote_url": self.remote_url,
            "branch": self.branch_name,
            "resume_mode": self.resume,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        try:
            repo = Repo(self.repo_path)
            
            if repo.bare:
                self.logger.error("Repository is bare (no working directory)")
                self.operation_complete.emit(False, "Repository is bare (no working directory)")
                return
                
            if repo.is_dirty() and not self.resume:
                self.logger.warning("Repository has uncommitted changes (will not be processed)")
            
            # Get branch info and emit to UI
            branches, current_branch = self.get_branch_info(repo)
            self.branch_info_signal.emit(branches, current_branch)
            
            # Switch branch if requested
            if self.branch_name and self.branch_name != current_branch:
                try:
                    repo.git.checkout(self.branch_name)
                    self.logger.info(f"Switched to branch: {self.branch_name}")
                    current_branch = self.branch_name
                except GitCommandError as e:
                    self.logger.error(f"Failed to switch branch: {str(e)}")
                    self.operation_complete.emit(False, f"Failed to switch branch: {str(e)}")
                    return
            
            # Setup remote
            if not repo.remotes:
                self.logger.info("Creating new remote 'origin'")
                repo.create_remote('origin', self.remote_url)
            
            # Check remote connection
            if not self.check_remote_connection(repo):
                self.operation_complete.emit(False, "Failed to connect to remote repository")
                return
            
            # Get repository statistics
            stats = self.get_repo_stats(repo)
            self.stats_signal.emit(stats)
            
            # Get files to process
            untracked = repo.untracked_files
            changed = [item.a_path for item in repo.index.diff(None)]
            staged = [item.a_path for item in repo.index.diff('HEAD')]
            
            # If resuming, check state for already processed files
            if self.resume:
                state = self.state.load_state()
                if state and 'processed_files' in state:
                    processed = set(state['processed_files'])
                    untracked = [f for f in untracked if f not in processed]
                    changed = [f for f in changed if f not in processed]
                    staged = [f for f in staged if f not in processed]
                    self.logger.info(f"Resuming - skipping {len(processed)} already processed files")
            
            all_files = set(untracked + changed + staged)
            total_files = len(all_files)
            self.logger.info(f"TOTAL FILES TO PROCESS: {total_files}")
            
            if not total_files:
                self.logger.warning("No files to process - working directory clean")
                self.operation_complete.emit(True, "No files to process - working directory clean")
                return
            
            # Initialize file statuses
            for file in untracked:
                self.file_status_signal.emit(file, "untracked")
            for file in changed:
                self.file_status_signal.emit(file, "modified")
            for file in staged:
                self.file_status_signal.emit(file, "staged")
            
            remaining_paths = [os.path.join(repo.working_dir, f) for f in all_files]
            processed_files = 0
            start_time = time.time()
            
            while remaining_paths and self._is_running:
                chunk, chunk_size, remaining_paths = self.collect_files_to_process(
                    repo, remaining_paths, MAX_CHUNK_SIZE
                )
                
                if not chunk:
                    break
                    
                commit_msg = (
                    f"Added {len(chunk)} items (~{chunk_size/1024/1024:.2f}MB) "
                    f"[{processed_files + len(chunk)}/{total_files}]"
                )
                
                success = self.process_chunk(repo, chunk, commit_msg)
                
                if not success:
                    self.operation_complete.emit(False, "Failed to process chunk")
                    return
                    
                processed_files += len(chunk)
                self.progress_signal.emit(processed_files, total_files, f"Processed {processed_files}/{total_files} files")
                
                # Update progress file
                with open(os.path.join(LOG_DIR, "push_progress.txt"), "w") as f:
                    f.write(f"{processed_files}/{total_files} files processed\n")
                    f.write(f"Last update: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    elapsed = time.time() - start_time
                    if processed_files > 0:
                        remaining = (elapsed / processed_files) * (total_files - processed_files)
                        f.write(f"Estimated remaining time: {remaining:.1f} seconds\n")
            
            if self._is_running:
                # Clear state on successful completion
                self.state.clear_state()
                self.operation_complete.emit(True, "Operation completed successfully")
            else:
                self.operation_complete.emit(False, "Operation cancelled by user")
                
        except Exception as e:
            self.logger.error(f"Error in run: {str(e)}", exc_info=True)
            self.operation_complete.emit(False, f"Error: {str(e)}")

class FileTreeItem(QTreeWidgetItem):
    """Custom tree widget item for files with status tracking"""
    def __init__(self, path, status="unknown"):
        super().__init__()
        self.path = path
        self.status = status
        self.setText(0, os.path.basename(path))
        
        # Set icon based on file type
        if os.path.isdir(path):
            self.setIcon(0, QIcon.fromTheme("folder"))
        else:
            self.setIcon(0, QIcon.fromTheme("text-x-generic"))
        
        self.update_status(status)
    
    def update_status(self, new_status):
        """Update the status and appearance of this item"""
        self.status = new_status
        
        # Set color based on status
        if new_status == "untracked":
            self.setForeground(0, COLOR_UNTRACKED)
        elif new_status == "modified":
            self.setForeground(0, COLOR_MODIFIED)
        elif new_status == "staged":
            self.setForeground(0, COLOR_STAGED)
        elif new_status == "pushed":
            self.setForeground(0, COLOR_PUSHED)
        elif new_status == "ignored":
            self.setForeground(0, COLOR_IGNORED)
        elif new_status == "error":
            self.setForeground(0, COLOR_ERROR)
        elif new_status == "conflict":
            self.setForeground(0, COLOR_CONFLICT)
        else:
            self.setForeground(0, COLOR_DEFAULT)
        
        # Update tooltip
        self.setToolTip(0, f"{self.path}\nStatus: {self.status}")

class FilePreviewWidget(QTextEdit):
    """Widget for previewing file contents"""
    def __init__(self):
        super().__init__()
        self.setReadOnly(True)
        self.setLineWrapMode(QTextEdit.NoWrap)
        self.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #d4d4d4;
                font-family: Consolas, monospace;
                font-size: 10pt;
            }
        """)
        
    def preview_file(self, file_path):
        """Preview the contents of a file"""
        self.clear()
        
        if not os.path.isfile(file_path):
            self.setPlainText("Not a file or file doesn't exist")
            return
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                self.setPlainText(content)
                
                # Simple syntax highlighting for common file types
                ext = os.path.splitext(file_path)[1].lower()
                if ext in ('.py', '.txt', '.md', '.json', '.yaml', '.yml'):
                    self.highlight_syntax(ext)
                    
        except UnicodeDecodeError:
            self.setPlainText("Binary file - cannot preview")
        except Exception as e:
            self.setPlainText(f"Error reading file: {str(e)}")
    
    def highlight_syntax(self, file_ext):
        """Basic syntax highlighting"""
        cursor = self.textCursor()
        format = QTextCharFormat()
        
        # Python highlighting
        if file_ext == '.py':
            # Highlight keywords
            keywords = ['def', 'class', 'import', 'from', 'as', 'if', 'else', 
                      'elif', 'for', 'while', 'try', 'except', 'finally', 
                      'with', 'return', 'yield', 'lambda', 'True', 'False', 
                      'None', 'and', 'or', 'not', 'is', 'in']
            
            format.setForeground(QColor(86, 156, 214))  # Blue
            for word in keywords:
                self.highlight_word(word, format)
            
            # Highlight strings
            format.setForeground(QColor(206, 145, 120))  # Orange
            self.highlight_pattern(r'"[^"]*"', format)
            self.highlight_pattern(r"'[^']*'", format)
            
            # Highlight numbers
            format.setForeground(QColor(181, 206, 168))  # Green
            self.highlight_pattern(r'\b\d+\b', format)
        
        # JSON highlighting
        elif file_ext == '.json':
            format.setForeground(QColor(206, 145, 120))  # Orange
            self.highlight_pattern(r'"[^"]*"\s*:', format)  # Keys
            
            format.setForeground(QColor(181, 206, 168))  # Green
            self.highlight_pattern(r':\s*"[^"]*"', format)  # String values
            
            format.setForeground(QColor(86, 156, 214))  # Blue
            self.highlight_pattern(r':\s*\b(true|false|null)\b', format, True)  # Literals
            
            format.setForeground(QColor(181, 206, 168))  # Green
            self.highlight_pattern(r':\s*\d+', format)  # Numbers
    
    def highlight_word(self, word, format):
        """Highlight all occurrences of a word"""
        cursor = self.textCursor()
        cursor.movePosition(QTextCursor.Start)
        
        while True:
            cursor = self.document().find(word, cursor)
            if cursor.isNull():
                break
            cursor.mergeCharFormat(format)
    
    def highlight_pattern(self, pattern, format, word=False):
        """Highlight text matching a regex pattern"""
        import re
        cursor = self.textCursor()
        cursor.movePosition(QTextCursor.Start)
        
        text = self.toPlainText()
        for match in re.finditer(pattern, text):
            start, end = match.span()
            cursor.setPosition(start)
            cursor.movePosition(QTextCursor.Right, QTextCursor.KeepAnchor, end - start)
            cursor.mergeCharFormat(format)

class StatsWidget(QWidget):
    """Widget for displaying repository statistics"""
    def __init__(self):
        super().__init__()
        self.layout = QFormLayout()
        self.setLayout(self.layout)
        
        # Initialize all labels
        self.labels = {
            'total_files': QLabel("0"),
            'total_size': QLabel("0 bytes"),
            'file_types': QLabel(""),
            'last_commit': QLabel("Never"),
            'branch_count': QLabel("0"),
            'remote_count': QLabel("0"),
            'untracked_files': QLabel("0"),
            'modified_files': QLabel("0"),
            'staged_files': QLabel("0")
        }
        
        # Add rows to form
        self.layout.addRow("Total Files:", self.labels['total_files'])
        self.layout.addRow("Total Size:", self.labels['total_size'])
        self.layout.addRow("File Types:", self.labels['file_types'])
        self.layout.addRow("Last Commit:", self.labels['last_commit'])
        self.layout.addRow("Branches:", self.labels['branch_count'])
        self.layout.addRow("Remotes:", self.labels['remote_count'])
        self.layout.addRow("Untracked Files:", self.labels['untracked_files'])
        self.layout.addRow("Modified Files:", self.labels['modified_files'])
        self.layout.addRow("Staged Files:", self.labels['staged_files'])
        
        # Set alignment
        for label in self.labels.values():
            label.setAlignment(Qt.AlignRight)
    
    def update_stats(self, stats):
        """Update the statistics display"""
        if not stats:
            return
            
        self.labels['total_files'].setText(str(stats.get('total_files', 0)))
        
        # Format size
        size = stats.get('total_size', 0)
        size_str = f"{size} bytes"
        if size > 1024 * 1024:
            size_str = f"{size/1024/1024:.2f} MB"
        elif size > 1024:
            size_str = f"{size/1024:.2f} KB"
        self.labels['total_size'].setText(size_str)
        
        # Format file types
        file_types = stats.get('file_types', {})
        top_types = sorted(file_types.items(), key=lambda x: x[1], reverse=True)[:5]
        type_str = ", ".join(f"{ext}: {count}" for ext, count in top_types)
        if len(file_types) > 5:
            type_str += f" (+{len(file_types)-5} more)"
        self.labels['file_types'].setText(type_str)
        
        # Format last commit
        last_commit = stats.get('last_commit', None)
        if last_commit:
            try:
                dt = datetime.strptime(last_commit, '%Y-%m-%d %H:%M:%S')
                self.labels['last_commit'].setText(dt.strftime('%b %d, %Y %H:%M'))
            except:
                self.labels['last_commit'].setText(last_commit)
        else:
            self.labels['last_commit'].setText("Never")
        
        # Update remaining stats
        self.labels['branch_count'].setText(str(stats.get('branch_count', 0)))
        self.labels['remote_count'].setText(str(stats.get('remote_count', 0)))
        self.labels['untracked_files'].setText(str(stats.get('untracked_files', 0)))
        self.labels['modified_files'].setText(str(stats.get('modified_files', 0)))
        self.labels['staged_files'].setText(str(stats.get('staged_files', 0)))

class GitPusherUI(QMainWindow):
    """Main application window for Git Repository Pusher V8"""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Git Repository Pusher V8")
        self.setGeometry(100, 100, 1200, 800)
        
        # Initialize settings
        self.settings = QSettings(CONFIG_FILE, QSettings.IniFormat)
        
        # Initialize UI
        self.init_ui()
        
        # Thread for git operations
        self.git_thread = None
        
        # File items cache
        self.file_items = {}
        
        # Load application icon
        self.setWindowIcon(QIcon.fromTheme("git"))
        
        # Load saved settings
        self.load_settings()
    
    def init_ui(self):
        """Initialize the user interface"""
        # Main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout()
        main_widget.setLayout(main_layout)
        
        # Splitter for resizable panels
        splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(splitter)
        
        # Top panel (controls and file tree)
        top_panel = QWidget()
        top_layout = QVBoxLayout()
        top_panel.setLayout(top_layout)
        splitter.addWidget(top_panel)
        
        # Bottom panel (logs and preview)
        bottom_panel = QTabWidget()
        splitter.addWidget(bottom_panel)
        
        # Add some stretch to the splitter
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        
        # Tab 1: Logs
        log_tab = QWidget()
        log_layout = QVBoxLayout()
        log_tab.setLayout(log_layout)
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #d4d4d4;
                font-family: Consolas, monospace;
                font-size: 10pt;
            }
        """)
        log_layout.addWidget(self.log_output)
        bottom_panel.addTab(log_tab, "Logs")
        
        # Tab 2: File Preview
        preview_tab = QWidget()
        preview_layout = QVBoxLayout()
        preview_tab.setLayout(preview_layout)
        
        self.file_preview = FilePreviewWidget()
        preview_layout.addWidget(self.file_preview)
        bottom_panel.addTab(preview_tab, "File Preview")
        
        # Tab 3: Statistics
        stats_tab = QWidget()
        stats_layout = QVBoxLayout()
        stats_tab.setLayout(stats_layout)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        
        self.stats_widget = StatsWidget()
        scroll.setWidget(self.stats_widget)
        
        stats_layout.addWidget(scroll)
        bottom_panel.addTab(stats_tab, "Statistics")
        
        # Control panel
        control_panel = QGroupBox("Repository Controls")
        control_layout = QHBoxLayout()
        control_panel.setLayout(control_layout)
        top_layout.addWidget(control_panel)
        
        # Repository path input
        self.repo_label = QLabel("Repository Path:")
        control_layout.addWidget(self.repo_label)
        
        self.repo_input = QLineEdit()
        self.repo_input.setPlaceholderText("Path to git repository")
        control_layout.addWidget(self.repo_input)
        
        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse_repository)
        control_layout.addWidget(self.browse_button)
        
        # Remote URL input
        self.remote_label = QLabel("Remote URL:")
        control_layout.addWidget(self.remote_label)
        
        self.remote_input = QLineEdit()
        self.remote_input.setPlaceholderText("git@github.com:user/repo.git")
        control_layout.addWidget(self.remote_input)
        
        # Branch selection
        self.branch_label = QLabel("Branch:")
        control_layout.addWidget(self.branch_label)
        
        self.branch_combo = QComboBox()
        self.branch_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        control_layout.addWidget(self.branch_combo)
        
        # Options panel
        options_panel = QGroupBox("Options")
        options_layout = QHBoxLayout()
        options_panel.setLayout(options_layout)
        top_layout.addWidget(options_panel)
        
        # Resume checkbox
        self.resume_check = QCheckBox("Resume previous operation")
        options_layout.addWidget(self.resume_check)
        
        # Theme toggle
        self.theme_button = QPushButton("Toggle Dark/Light")
        self.theme_button.clicked.connect(self.toggle_theme)
        options_layout.addWidget(self.theme_button)
        
        # Start/Stop buttons
        button_layout = QHBoxLayout()
        options_layout.addLayout(button_layout)
        
        self.start_button = QPushButton("Start Pushing")
        self.start_button.setStyleSheet("background-color: #4CAF50; color: white;")
        self.start_button.clicked.connect(self.start_pushing)
        button_layout.addWidget(self.start_button)
        
        self.stop_button = QPushButton("Stop")
        self.stop_button.setStyleSheet("background-color: #f44336; color: white;")
        self.stop_button.clicked.connect(self.stop_pushing)
        self.stop_button.setEnabled(False)
        button_layout.addWidget(self.stop_button)
        
        # File tree
        self.file_tree = QTreeWidget()
        self.file_tree.setHeaderLabel("Repository Files")
        self.file_tree.setColumnCount(1)
        self.file_tree.setStyleSheet("QTreeWidget { font-family: monospace; }")
        self.file_tree.itemClicked.connect(self.on_file_clicked)
        top_layout.addWidget(self.file_tree)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        top_layout.addWidget(self.progress_bar)
        
        # Status label
        self.status_label = QLabel("Ready")
        self.status_label.setAlignment(Qt.AlignCenter)
        top_layout.addWidget(self.status_label)
        
        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")
        
        # Set initial theme
        self.set_theme(self.settings.value("theme", "dark"))
    
    def set_theme(self, theme):
        """Set the application theme (dark or light)"""
        palette = QPalette()
        
        if theme == "dark":
            palette.setColor(QPalette.Window, QColor(53, 53, 53))
            palette.setColor(QPalette.WindowText, Qt.white)
            palette.setColor(QPalette.Base, QColor(25, 25, 25))
            palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
            palette.setColor(QPalette.ToolTipBase, Qt.white)
            palette.setColor(QPalette.ToolTipText, Qt.white)
            palette.setColor(QPalette.Text, Qt.white)
            palette.setColor(QPalette.Button, QColor(53, 53, 53))
            palette.setColor(QPalette.ButtonText, Qt.white)
            palette.setColor(QPalette.BrightText, Qt.red)
            palette.setColor(QPalette.Link, QColor(42, 130, 218))
            palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
            palette.setColor(QPalette.HighlightedText, Qt.black)
        else:
            palette = QApplication.style().standardPalette()
        
        QApplication.setPalette(palette)
        self.settings.setValue("theme", theme)
    
    def toggle_theme(self):
        """Toggle between dark and light theme"""
        current_theme = self.settings.value("theme", "dark")
        new_theme = "light" if current_theme == "dark" else "dark"
        self.set_theme(new_theme)
    
    def load_settings(self):
        """Load saved settings"""
        self.repo_input.setText(self.settings.value("repo_path", ""))
        self.remote_input.setText(self.settings.value("remote_url", ""))
        self.resume_check.setChecked(self.settings.value("resume", False, type=bool))
    
    def save_settings(self):
        """Save current settings"""
        self.settings.setValue("repo_path", self.repo_input.text())
        self.settings.setValue("remote_url", self.remote_input.text())
        self.settings.setValue("resume", self.resume_check.isChecked())
    
    def browse_repository(self):
        """Open a directory dialog to select the repository"""
        dir_path = QFileDialog.getExistingDirectory(self, "Select Git Repository")
        if dir_path:
            self.repo_input.setText(dir_path)
            self.save_settings()
            
            # Try to detect remote URL and branches
            try:
                repo = Repo(dir_path)
                
                # Get remote URL
                if repo.remotes:
                    remote = repo.remotes[0]
                    self.remote_input.setText(remote.url)
                
                # Get branches
                self.update_branch_list(repo)
                
                # Update statistics
                self.update_repo_stats(repo)
                
            except Exception as e:
                self.log_message(f"Error loading repository info: {str(e)}", "error")
    
    def update_branch_list(self, repo):
        """Update the branch dropdown list"""
        self.branch_combo.clear()
        
        try:
            branches = [str(branch) for branch in repo.branches]
            current_branch = repo.active_branch.name if not repo.head.is_detached else "DETACHED"
            
            self.branch_combo.addItems(branches)
            
            if current_branch in branches:
                self.branch_combo.setCurrentIndex(branches.index(current_branch))
            
        except Exception as e:
            self.log_message(f"Error getting branches: {str(e)}", "error")
    
    def update_repo_stats(self, repo):
        """Update repository statistics"""
        try:
            stats = {
                'total_files': 0,
                'total_size': 0,
                'file_types': defaultdict(int),
                'last_commit': str(repo.head.commit.authored_datetime) if not repo.bare else None,
                'branch_count': len(list(repo.branches)),
                'remote_count': len(repo.remotes),
                'untracked_files': len(repo.untracked_files),
                'modified_files': len([item.a_path for item in repo.index.diff(None)]),
                'staged_files': len([item.a_path for item in repo.index.diff('HEAD')])
            }
            
            # Count files and sizes
            for root, _, files in os.walk(repo.working_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        size = os.path.getsize(file_path)
                        stats['total_files'] += 1
                        stats['total_size'] += size
                        
                        ext = os.path.splitext(file)[1].lower()
                        if ext:
                            stats['file_types'][ext] += 1
                    except OSError:
                        pass
            
            self.stats_widget.update_stats(stats)
            
        except Exception as e:
            self.log_message(f"Error collecting stats: {str(e)}", "error")
    
    def validate_inputs(self):
        """Validate the user inputs"""
        repo_path = self.repo_input.text().strip()
        remote_url = self.remote_input.text().strip()
        
        if not repo_path:
            QMessageBox.warning(self, "Error", "Please enter a repository path")
            return False
        
        if not os.path.isdir(repo_path):
            QMessageBox.warning(self, "Error", f"Directory does not exist: {repo_path}")
            return False
        
        if not remote_url:
            QMessageBox.warning(self, "Error", "Please enter a remote URL")
            return False
        
        if not remote_url.startswith(('http://', 'https://', 'git@')):
            QMessageBox.warning(
                self, 
                "Error", 
                "Remote URL should be a valid git repository URL\n\n"
                "Examples:\n"
                "https://github.com/user/repo.git\n"
                "git@github.com:user/repo.git"
            )
            return False
        
        return True
    
    def start_pushing(self):
        """Start the git push operation"""
        if not self.validate_inputs():
            return
        
        # Clear previous state
        self.file_tree.clear()
        self.file_items = {}
        self.log_output.clear()
        
        # Disable controls during operation
        self.repo_input.setEnabled(False)
        self.remote_input.setEnabled(False)
        self.browse_button.setEnabled(False)
        self.branch_combo.setEnabled(False)
        self.resume_check.setEnabled(False)
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        
        # Get selected branch
        branch_name = self.branch_combo.currentText() if self.branch_combo.count() > 0 else None
        
        # Create and start the git thread
        self.git_thread = GitPusherThread(
            self.repo_input.text().strip(),
            self.remote_input.text().strip(),
            branch_name,
            self.resume_check.isChecked()
        )
        
        # Connect signals
        self.git_thread.progress_signal.connect(self.update_progress)
        self.git_thread.file_status_signal.connect(self.update_file_status)
        self.git_thread.operation_complete.connect(self.operation_completed)
        self.git_thread.log_signal.connect(self.log_message)
        self.git_thread.branch_info_signal.connect(self.update_branch_display)
        self.git_thread.stats_signal.connect(self.stats_widget.update_stats)
        self.git_thread.conflict_detected.connect(self.handle_conflict)
        
        # Start the thread
        self.git_thread.start()
        
        self.status_bar.showMessage("Operation started...")
        self.save_settings()
    
    def stop_pushing(self):
        """Stop the current git operation"""
        if self.git_thread and self.git_thread.isRunning():
            self.git_thread.stop()
            self.status_bar.showMessage("Stopping operation...")
    
    def update_progress(self, current, total, message):
        """Update the progress bar and status"""
        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)
            percent = int((current / total) * 100)
            self.progress_bar.setFormat(f"{percent}% - {message}")
        else:
            self.progress_bar.setMaximum(100)
            self.progress_bar.setValue(0)
            self.progress_bar.setFormat(message)
        
        self.status_label.setText(message)
    
    def update_file_status(self, file_path, status):
        """Update the status of a file in the tree"""
        # Create a hierarchical structure for the path
        parts = file_path.split(os.sep)
        parent = None
        
        # Build the path hierarchy
        current_path = ""
        for part in parts:
            current_path = os.path.join(current_path, part) if current_path else part
            
            if current_path not in self.file_items:
                item = FileTreeItem(current_path, status)
                self.file_items[current_path] = item
                
                if parent is None:
                    self.file_tree.addTopLevelItem(item)
                else:
                    parent.addChild(item)
                
                parent = item
            else:
                parent = self.file_items[current_path]
                parent.update_status(status)
        
        # Expand the tree to show the updated item
        if parent:
            item = parent
            while item.parent():
                item.parent().setExpanded(True)
                item = item.parent()
            self.file_tree.scrollToItem(parent)
    
    def on_file_clicked(self, item):
        """Handle file click event to show preview"""
        if isinstance(item, FileTreeItem):
            repo_path = self.repo_input.text().strip()
            if repo_path:
                full_path = os.path.join(repo_path, item.path)
                self.file_preview.preview_file(full_path)
    
    def handle_conflict(self, file_path):
        """Handle merge conflict detection"""
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Warning)
        msg.setWindowTitle("Merge Conflict Detected")
        msg.setText(f"Merge conflict detected in file:\n{file_path}")
        msg.setInformativeText("How would you like to proceed?")
        
        abort_btn = msg.addButton("Abort Operation", QMessageBox.RejectRole)
        skip_btn = msg.addButton("Skip This File", QMessageBox.ActionRole)
        resolve_btn = msg.addButton("Open in Editor", QMessageBox.ActionRole)
        
        msg.exec_()
        
        if msg.clickedButton() == abort_btn:
            self.stop_pushing()
        elif msg.clickedButton() == skip_btn:
            # Mark file as conflict but continue
            self.update_file_status(file_path, "conflict")
        elif msg.clickedButton() == resolve_btn:
            # Try to open file in default editor
            repo_path = self.repo_input.text().strip()
            if repo_path:
                full_path = os.path.join(repo_path, file_path)
                try:
                    if platform.system() == 'Windows':
                        os.startfile(full_path)
                    elif platform.system() == 'Darwin':
                        os.system(f'open "{full_path}"')
                    else:
                        os.system(f'xdg-open "{full_path}"')
                except Exception as e:
                    self.log_message(f"Could not open file: {str(e)}", "error")
    
    def update_branch_display(self, branches, current_branch):
        """Update the branch dropdown list from thread"""
        self.branch_combo.clear()
        self.branch_combo.addItems(branches)
        
        if current_branch in branches:
            self.branch_combo.setCurrentIndex(branches.index(current_branch))
    
    def operation_completed(self, success, message):
        """Handle operation completion"""
        # Re-enable controls
        self.repo_input.setEnabled(True)
        self.remote_input.setEnabled(True)
        self.browse_button.setEnabled(True)
        self.branch_combo.setEnabled(True)
        self.resume_check.setEnabled(True)
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        
        # Show completion message
        if success:
            self.status_bar.showMessage("Operation completed successfully")
            QMessageBox.information(self, "Success", "Operation completed successfully")
        else:
            self.status_bar.showMessage("Operation completed with errors")
            QMessageBox.warning(self, "Error", message)
        
        # Update repository info
        repo_path = self.repo_input.text().strip()
        if repo_path and os.path.isdir(repo_path):
            try:
                repo = Repo(repo_path)
                self.update_branch_list(repo)
                self.update_repo_stats(repo)
            except:
                pass
    
    def log_message(self, message, level):
        """Add a message to the log output"""
        # Color code based on log level
        if level == "error":
            color = "#ff4444"
        elif level == "warning":
            color = "#ffbb33"
        elif level == "info":
            color = "#33b5e5"
        else:
            color = "#aaaaaa"
        
        # Append the message with HTML formatting
        self.log_output.append(f'<span style="color:{color}">{message}</span>')
        
        # Auto-scroll to bottom
        self.log_output.verticalScrollBar().setValue(
            self.log_output.verticalScrollBar().maximum()
        )
    
    def closeEvent(self, event):
        """Handle window close event"""
        self.save_settings()
        
        if self.git_thread and self.git_thread.isRunning():
            reply = QMessageBox.question(
                self, 
                "Operation in Progress", 
                "A git operation is still running. Are you sure you want to quit?", 
                QMessageBox.Yes | QMessageBox.No, 
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.git_thread.stop()
                self.git_thread.wait(2000)  # Wait up to 2 seconds
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()

def main():
    """Main function to run the application"""
    app = QApplication([])
    
    # Set modern style
    app.setStyle('Fusion')
    
    # Create and show the main window
    window = GitPusherUI()
    window.show()
    
    # Run the application
    app.exec_()

if __name__ == "__main__":
    main()
        