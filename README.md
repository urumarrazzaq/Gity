# Gity

## Overview
Gity is a powerful and user-friendly Git pushing tool designed to help developers efficiently push large Unreal Engine 5 (UE5) projects to GitHub without relying on Git Large File Storage (LFS) or encountering GitHub's storage limitations.

Managing large UE5 projects in GitHub can be challenging due to file size restrictions and performance issues when dealing with large binary files. Gity streamlines this process by providing an optimized and automated workflow for handling and pushing large repositories.

## Purpose
Unreal Engine 5 projects contain large binary files such as textures, models, and assets that often exceed GitHub's standard file size limits (100MB per file) and repository storage limits (5GB for free accounts). Developers traditionally rely on Git LFS to manage these files, but LFS has its own limitations and complexities, especially when working in collaborative environments.

Gity provides a solution by optimizing the push process, handling large files efficiently, and allowing users to resume operations without losing progress. This makes it a valuable tool for game developers, 3D artists, and teams working with large repositories.

## Features
- **Resume Previous Operations** – Avoid losing progress if an upload is interrupted.
- **Optimized File Handling** – Streamlined pushing of large files without Git LFS.
- **Git Repository Management** – Easily browse, track, and manage your repository.
- **Dark/Light Mode** – Toggle between themes for better usability.
- **Progress Monitoring** – Real-time status updates and detailed logs.
- **Branch & Remote Management** – Automatically detects and updates remote branches.
- **Error Handling & Conflict Resolution** – Detects merge conflicts and provides resolution options.

## Installation
Gity is built using **PyQt5** for the UI and **GitPython** for repository handling. To install and run Gity, follow these steps:

### Prerequisites
Ensure you have the following installed:
- **Python 3.7+**
- **Git** (must be installed and added to system PATH)

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run Gity
```bash
python gity.py
```

## Usage
1. **Select Repository** – Click "Browse" to select your Unreal Engine project folder.
2. **Set Remote URL** – Enter your GitHub repository URL.
3. **Choose Branch** – Select or create a branch for pushing files.
4. **Enable Resume (Optional)** – Check "Resume previous operation" if needed.
5. **Start Push** – Click "Start Pushing" and monitor progress.
6. **Handle Conflicts** – If conflicts arise, choose to resolve or skip files.
7. **Complete Process** – Once completed, your UE5 project is safely uploaded.

## How Gity Solves GitHub Storage Issues
- **Splits Large Pushes into Chunks** – Ensures that GitHub doesn’t reject large commits.
- **Bypasses Git LFS** – Avoids the complexity and limits of Git Large File Storage.
- **Auto-Resume Feature** – Prevents unnecessary reuploads in case of interruptions.
- **Smart Compression & Caching** – Reduces file size impact for more efficient uploads.

## Contributing
Contributions are welcome! Feel free to submit issues or pull requests to improve Gity.

## Screenshot
![Screenshot 2025-04-04 163401](https://github.com/user-attachments/assets/35b5ea4c-3cc6-4492-91e1-2e47df51b6b4)

## License
MIT License

---
### 🚀 Gity – Your Ultimate UE5 Git Pushing Solution!

