# project_to_file_backend.py
import os
import sys
from pathlib import Path

# --- Configuration ---

# Directories to exclude from both structure and content
EXCLUDED_DIRS = [
    '__pycache__',
    '.git',
    '.venv',        # Common virtual environment names
    'venv',
    'env',
    '.env',         # Exclude the .env file itself if it contains secrets
    'build',
    'dist',
    '*.egg-info',   # Python packaging artifacts
    '.pytest_cache',
    '.mypy_cache',
    '.vscode',
    '.idea',
    'node_modules', # If you happen to have node_modules
    'migrations',   # Often contains auto-generated code, review if needed
    'alembic'       # Alembic directory
]

# Specific files to exclude
EXCLUDED_FILES = [
    '.DS_Store',
    '*.pyc',
    '*.pyo',
    '*.pyd',
    '.env',         # Explicitly exclude again just in case
    'project_to_file_backend.py', # Exclude this script itself
    'project_structure_backend.txt', # Exclude the output file
    # Add any other specific files like local config overrides
]

# File extensions to treat as binary/media (content won't be included)
# Add more as needed (e.g., .jpg, .gif, .mp4, .db, .sqlite)
BINARY_EXTENSIONS = [
    '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp', '.ico',
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
    '.zip', '.tar', '.gz', '.rar',
    '.mp3', '.wav', '.ogg',
    '.mp4', '.avi', '.mov', '.wmv',
    '.db', '.sqlite', '.sqlite3',
    '.pkl', '.joblib',
    '.pt', '.pth', '.onnx', # Model files
]

# Output file name
OUTPUT_FILE = 'project_structure_backend.txt'

# --- Script Logic ---

file_structure_tree = ""
file_contents = ""

def should_exclude(path: Path) -> bool:
    """Check if a path should be excluded based on configured lists."""
    # Check against excluded directory names
    if any(part in EXCLUDED_DIRS for part in path.parts):
        return True
    # Check against excluded directory patterns (like *.egg-info)
    if any(path.match(pattern) for pattern in EXCLUDED_DIRS if '*' in pattern):
         return True
    # Check against excluded file names and patterns
    if path.name in EXCLUDED_FILES:
        return True
    if any(path.match(pattern) for pattern in EXCLUDED_FILES if '*' in pattern):
        return True

    return False

def traverse_directory(dir_path: Path, indent: str = ''):
    """Recursively traverses directories and builds the structure/content strings."""
    global file_structure_tree
    global file_contents

    try:
        # Sort entries for consistent ordering: directories first, then files
        entries = sorted(list(dir_path.iterdir()), key=lambda p: (p.is_file(), p.name.lower()))
    except PermissionError:
        print(f"Warning: Permission denied for directory '{dir_path}'. Skipping.")
        return
    except FileNotFoundError:
         print(f"Warning: Directory '{dir_path}' not found during traversal (might have been deleted). Skipping.")
         return


    for entry in entries:
        if should_exclude(entry):
            continue

        if entry.is_dir():
            file_structure_tree += f"{indent}{entry.name}/\n"
            traverse_directory(entry, indent + '  ')
        elif entry.is_file():
            file_structure_tree += f"{indent}{entry.name}\n"
            file_extension = entry.suffix.lower()

            # Add separators for all files
            file_contents += f"\n---------- {entry.name} ----------\n"
            if file_extension in BINARY_EXTENSIONS:
                file_contents += f"(Binary file type {file_extension} - content not included)\n"
            else:
                try:
                    # Try reading with UTF-8 first
                    with entry.open('r', encoding='utf-8') as f:
                        file_contents += f.read() + "\n"
                except UnicodeDecodeError:
                    try:
                        # Fallback to latin-1 if UTF-8 fails
                        with entry.open('r', encoding='latin-1') as f:
                            file_contents += f.read() + "\n"
                        file_contents += "(Warning: Read file using latin-1 encoding due to UTF-8 decode error)\n"
                    except Exception as read_error_fallback:
                         file_contents += f"ERROR READING FILE (Fallback Failed): {read_error_fallback}\n"
                except Exception as read_error:
                    file_contents += f"ERROR READING FILE: {read_error}\n"

            file_contents += f"---------- END {entry.name} ----------\n\n"


def generate_project_structure_and_content(project_root: Path, output_file_path: Path):
    """Generates the combined structure and content file."""
    global file_structure_tree
    global file_contents
    file_structure_tree = "" # Reset global state
    file_contents = ""     # Reset global state

    print(f"Starting traversal from: {project_root}")
    traverse_directory(project_root)

    full_output = f"--- START OF FILE {output_file_path.name} ---\n\n"
    full_output += file_structure_tree
    full_output += "\n" # Separator between tree and content
    full_output += file_contents
    full_output += f"--- END OF FILE {output_file_path.name} ---\n"


    try:
        with output_file_path.open('w', encoding='utf-8') as f:
            f.write(full_output)
        print(f"Project structure and content written to '{output_file_path}'")
    except IOError as e:
        print(f"Error writing to output file '{output_file_path}': {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during file writing: {e}", file=sys.stderr)
        sys.exit(1)

# --- Main Execution ---
if __name__ == "__main__":
    project_root_path = Path(os.getcwd()) # Get current working directory as Path object
    output_path = project_root_path / OUTPUT_FILE

    print(f"Project Root: {project_root_path}")
    print(f"Output File: {output_path}")

    if not project_root_path.is_dir():
        print(f"Error: Project root directory '{project_root_path}' not found or is not a directory.", file=sys.stderr)
        sys.exit(1)

    try:
        generate_project_structure_and_content(project_root_path, output_path)
    except Exception as e:
        print(f"\nAn unexpected error occurred during execution: {e}", file=sys.stderr)
        sys.exit(1)