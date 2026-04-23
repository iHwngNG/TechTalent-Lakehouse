import os
import sys
from pathlib import Path

def find_project_root(start_path: str = None, markers: tuple = (".env", ".git", "README.md")) -> Path:
    """
    Find the project root directory by traversing up the directory tree.
    
    Args:
        start_path: Starting directory path (defaults to current working directory)
        markers: Tuple of file/directory names that indicate the project root
        
    Returns:
        Path object pointing to the project root directory
        
    Raises:
        FileNotFoundError: If project root cannot be found
    """
    if start_path is None:
        # Try to get the file path, fallback to current working directory
        try:
            start_path = Path(__file__).resolve().parent
        except NameError:
            # __file__ not available (e.g., in Databricks notebooks)
            start_path = Path(os.getcwd()).resolve()
    else:
        start_path = Path(start_path).resolve()
    
    current = start_path
    
    # Traverse up the directory tree
    while current != current.parent:
        # Check if any of the markers exist in the current directory
        if any((current / marker).exists() for marker in markers):
            return current
        current = current.parent
    
    # Check root directory
    if any((current / marker).exists() for marker in markers):
        return current
    
    raise FileNotFoundError(f"Could not find project root starting from {start_path}")

def update_env_project_root(project_root: Path = None):
    """
    Update the PROJECT_ROOT variable in the .env file with the actual project root path.
    
    Args:
        project_root: Path to the project root (if None, will be auto-detected)
    """
    try:
        if project_root is None:
            project_root = find_project_root()
        
        env_file_path = project_root / ".env"
        
        if not env_file_path.exists():
            print(f"Warning: .env file not found at {env_file_path}")
            return
        
        # Read current .env content
        with open(env_file_path, 'r') as f:
            lines = f.readlines()
        
        # Update or add PROJECT_ROOT
        project_root_line = f"PROJECT_ROOT={project_root}\n"
        updated = False
        
        for i, line in enumerate(lines):
            if line.strip().startswith("PROJECT_ROOT"):
                lines[i] = project_root_line
                updated = True
                break
        
        if not updated:
            # Add PROJECT_ROOT at the beginning
            lines.insert(0, project_root_line)
        
        # Write back to .env file
        with open(env_file_path, 'w') as f:
            f.writelines(lines)
        
        print(f"✓ Updated PROJECT_ROOT in .env to: {project_root}")
        return project_root
        
    except Exception as e:
        print(f"Error updating .env file: {e}")
        raise

if __name__ == "__main__":
    # Find and display project root
    try:
        # Try to use the current file's directory, fallback to current working directory
        try:
            current_dir = Path(__file__).resolve().parent
        except NameError:
            # __file__ not available in Databricks
            current_dir = Path(os.getcwd()).resolve()
        
        project_root = find_project_root(start_path=current_dir)
        print(f"Starting from: {current_dir}")
        print(f"Project root: {project_root}")
        
        # Update .env file
        update_env_project_root(project_root)
    except FileNotFoundError as e:
        print(f"Error: {e}")
