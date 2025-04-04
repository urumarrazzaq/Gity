import os
import time
import hashlib
import logging
from pathlib import Path
from collections import deque
from typing import List, Tuple, Optional, Set, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from git import Repo, GitCommandError

# PyQt5 imports for UI
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTreeWidget, QTreeWidgetItem, 
                            QVBoxLayout, QWidget, QLabel, QPushButton, QLineEdit, 
                            QProgressBar, QHBoxLayout, QMessageBox, QFileDialog, 
                            QSplitter, QTextEdit, QStatusBar)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QIcon, QColor, QFont

# Constants
MAX_CHUNK_SIZE = 25 * 1024 * 1024  # 25MB
MAX_PUSH_RETRIES = 3
MIN_COMMIT_INTERVAL = 4  # seconds between commits
MAX_COMMIT_MESSAGE_LENGTH = 100
MAX_WORKERS = 4
LOG_DIR = "push_logs"

# Color constants for UI
COLOR_UNTRACKED = QColor(255, 165, 0)  # Orange
COLOR_MODIFIED = QColor(255, 255, 0)    # Yellow
COLOR_STAGED = QColor(100, 149, 237)    # Cornflower blue
COLOR_PUSHED = QColor(50, 205, 50)      # Lime green
COLOR_IGNORED = QColor(169, 169, 169)   # Dark gray
COLOR_ERROR = QColor(255, 0, 0)         # Red
COLOR_DEFAULT = QColor(255, 255, 255)   # White

class GitPusherThread(QThread):
    """Thread for running the git push operations"""
    progress_signal = pyqtSignal(int, int, str)  # current, total, message
    file_status_signal = pyqtSignal(str, str)    # file_path, status
    operation_complete = pyqtSignal(bool, str)   # success, message
    log_signal = pyqtSignal(str, str)           # message, level
    
    def __init__(self, repo_path, remote_url):
        super().__init__()
        self.repo_path = repo_path
        self.remote_url = remote_url
        self._is_running = True
        
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
    
    def process_chunk(self, repo: Repo, chunk: List[str], commit_message: str) -> bool:
        """Process a chunk with detailed logging"""
        chunk_size = sum(self.get_file_size(os.path.join(repo.working_dir, f)) for f in chunk)
        
        self.log_operation_summary("processing_chunk", {
            "file_count": len(chunk),
            "total_size": f"{chunk_size/1024/1024:.2f}MB",
            "first_file": chunk[0] if chunk else "N/A",
            "commit_message": commit_message
        })
        
        try:
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
    
    def stop(self):
        """Stop the thread gracefully"""
        self._is_running = False
    
    def run(self):
        """Main processing function"""
        self._is_running = True
        
        self.log_operation_summary("repository_processing_start", {
            "repository_path": self.repo_path,
            "remote_url": self.remote_url,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        try:
            repo = Repo(self.repo_path)
            
            if repo.bare:
                self.logger.error("Repository is bare (no working directory)")
                self.operation_complete.emit(False, "Repository is bare (no working directory)")
                return
                
            if repo.is_dirty():
                self.logger.warning("Repository has uncommitted changes (will not be processed)")
            
            # Setup remote
            if not repo.remotes:
                self.logger.info("Creating new remote 'origin'")
                repo.create_remote('origin', self.remote_url)
            
            # Check remote connection
            if not self.check_remote_connection(repo):
                self.operation_complete.emit(False, "Failed to connect to remote repository")
                return
            
            # Get files to process
            untracked = repo.untracked_files
            changed = [item.a_path for item in repo.index.diff(None)]
            staged = [item.a_path for item in repo.index.diff('HEAD')]
            
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
        else:
            self.setForeground(0, COLOR_DEFAULT)
        
        # Update tooltip
        self.setToolTip(0, f"{self.path}\nStatus: {self.status}")

class GitPusherUI(QMainWindow):
    """Main application window for Git Repository Pusher V7"""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Git Repository Pusher V7")
        self.setGeometry(100, 100, 1000, 700)
        
        # Initialize UI
        self.init_ui()
        
        # Thread for git operations
        self.git_thread = None
        
        # File items cache
        self.file_items = {}
        
        # Load application icon
        self.setWindowIcon(QIcon.fromTheme("git"))
    
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
        
        # Bottom panel (logs)
        bottom_panel = QWidget()
        bottom_layout = QVBoxLayout()
        bottom_panel.setLayout(bottom_layout)
        splitter.addWidget(bottom_panel)
        
        # Add some stretch to the splitter
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        
        # Input controls
        control_layout = QHBoxLayout()
        top_layout.addLayout(control_layout)
        
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
        
        # Start button
        self.start_button = QPushButton("Start Pushing")
        self.start_button.setStyleSheet("background-color: #4CAF50; color: white;")
        self.start_button.clicked.connect(self.start_pushing)
        control_layout.addWidget(self.start_button)
        
        # Stop button
        self.stop_button = QPushButton("Stop")
        self.stop_button.setStyleSheet("background-color: #f44336; color: white;")
        self.stop_button.clicked.connect(self.stop_pushing)
        self.stop_button.setEnabled(False)
        control_layout.addWidget(self.stop_button)
        
        # File tree
        self.file_tree = QTreeWidget()
        self.file_tree.setHeaderLabel("Repository Files")
        self.file_tree.setColumnCount(1)
        self.file_tree.setStyleSheet("QTreeWidget { font-family: monospace; }")
        top_layout.addWidget(self.file_tree)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        top_layout.addWidget(self.progress_bar)
        
        # Status label
        self.status_label = QLabel("Ready")
        self.status_label.setAlignment(Qt.AlignCenter)
        top_layout.addWidget(self.status_label)
        
        # Log output
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
        bottom_layout.addWidget(self.log_output)
        
        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")
    
    def browse_repository(self):
        """Open a directory dialog to select the repository"""
        dir_path = QFileDialog.getExistingDirectory(self, "Select Git Repository")
        if dir_path:
            self.repo_input.setText(dir_path)
            
            # Try to detect remote URL
            try:
                repo = Repo(dir_path)
                if repo.remotes:
                    remote = repo.remotes[0]
                    self.remote_input.setText(remote.url)
            except:
                pass
    
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
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        
        # Create and start the git thread
        self.git_thread = GitPusherThread(
            self.repo_input.text().strip(),
            self.remote_input.text().strip()
        )
        
        # Connect signals
        self.git_thread.progress_signal.connect(self.update_progress)
        self.git_thread.file_status_signal.connect(self.update_file_status)
        self.git_thread.operation_complete.connect(self.operation_completed)
        self.git_thread.log_signal.connect(self.log_message)
        
        # Start the thread
        self.git_thread.start()
        
        self.status_bar.showMessage("Operation started...")
    
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
    
    def operation_completed(self, success, message):
        """Handle operation completion"""
        # Re-enable controls
        self.repo_input.setEnabled(True)
        self.remote_input.setEnabled(True)
        self.browse_button.setEnabled(True)
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        
        # Show completion message
        if success:
            self.status_bar.showMessage("Operation completed successfully")
            QMessageBox.information(self, "Success", "Operation completed successfully")
        else:
            self.status_bar.showMessage("Operation completed with errors")
            QMessageBox.warning(self, "Error", message)
    
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