#!.venv/bin/python

import argparse
import os
import re
import sys

import nbformat


def find_forbidden_pattern(file_path, forbidden_pattern):
    """
    Check if a forbidden regex pattern exists in the given file.

    Args:
        file_path (str): Path to the file to check
        forbidden_pattern (str): Regex pattern to search for

    Returns:
        list: Lines matching the forbidden pattern
    """
    matches = []
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            for line_num, line in enumerate(file, 1):
                if re.search(forbidden_pattern, line):
                    matches.append((line_num, line.strip()))
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

    return matches


def check_jupyter_notebook(notebook_path, forbidden_pattern):
    """
    Check if a forbidden regex pattern exists in Jupyter notebook cells.

    Args:
        notebook_path (str): Path to the Jupyter notebook
        forbidden_pattern (str): Regex pattern to search for

    Returns:
        list: Cells matching the forbidden pattern
    """
    matches = []
    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            notebook = nbformat.read(f, as_version=4)

        for cell_num, cell in enumerate(notebook.cells, 1):
            if cell.cell_type in ["code", "markdown"]:
                # Search through all cell source lines
                cell_matches = re.findall(forbidden_pattern, cell.source)
                if cell_matches:
                    matches.append((cell_num, cell.source.split("\n")[0]))
    except Exception as e:
        print(f"Error reading notebook {notebook_path}: {e}")

    return matches


def main():
    """
    Checks if a string is present in any of the files.
    """
    parser = argparse.ArgumentParser(
        description="Check files for a forbidden regex pattern"
    )
    parser.add_argument("filenames", nargs="*", help="Filenames to check")
    parser.add_argument(
        "--forbidden-pattern",
        required=True,
        help="Regex pattern to search for in files",
    )
    parser.add_argument(
        "--ignore-case",
        action="store_true",
        help="Perform case-insensitive regex search",
    )

    args = parser.parse_args()

    # Compile regex pattern with optional case-insensitive flag
    regex_flags = re.IGNORECASE if args.ignore_case else 0
    try:
        compiled_pattern = re.compile(args.forbidden_pattern, regex_flags)
    except re.error as e:
        print(f"Invalid regex pattern: {e}")
        sys.exit(1)

    # List of file extensions to check
    text_extensions = [".py", ".txt", ".md", ".rst", ".csv", ".json", ".yml", ".yaml"]
    notebook_extensions = [".ipynb"]

    found_matches = False

    for filename in args.filenames:
        file_ext = os.path.splitext(filename)[1].lower()

        # Check text files
        if file_ext in text_extensions:
            text_matches = find_forbidden_pattern(filename, compiled_pattern)
            if text_matches:
                found_matches = True
                print(f"Forbidden pattern found in {filename}:")
                for line_num, match in text_matches:
                    print(f"  Line {line_num}: {match}")

        # Check Jupyter notebooks
        elif file_ext in notebook_extensions:
            notebook_matches = check_jupyter_notebook(filename, compiled_pattern)
            if notebook_matches:
                found_matches = True
                print(f"Forbidden pattern found in {filename}:")
                for cell_num, match in notebook_matches:
                    print(f"  Cell {cell_num}: {match}")

    # Exit with non-zero status if forbidden pattern is found
    if found_matches:
        print(f"Error: Forbidden pattern '{args.forbidden_pattern}' found in files.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
