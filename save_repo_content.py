import os
import pathlib


def should_include_file(file_path):
    # Files to exclude
    exclude_patterns = [
        ".git",
        "__pycache__",
        ".egg-info",
        ".venv",
        ".pyc",
        ".pyo",
        ".pyd",
        ".so",
        ".dll",
        ".coverage",
        ".pytest_cache",
        ".mypy_cache",
        ".DS_Store",
        "Thumbs.db",
        "save_repo_content.py",
        ".json",
    ]

    # Check if any pattern matches the file path
    return not any(pattern in str(file_path) for pattern in exclude_patterns)


def get_repo_structure(start_path=".", indent=""):
    structure = []

    try:
        # Sort directories first, then files
        items = sorted(
            pathlib.Path(start_path).iterdir(), key=lambda x: (not x.is_dir(), x.name)
        )

        for item in items:
            if not should_include_file(item):
                continue

            if item.is_dir():
                structure.append(f"{indent}📁 {item.name}/")
                structure.extend(get_repo_structure(item, indent + "  "))
            else:
                structure.append(f"{indent}📄 {item.name}")
    except PermissionError:
        return []

    return structure


def read_file_content(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Error reading file: {str(e)}"


def save_repo_content(output_file="repository_content.txt"):
    important_extensions = {
        ".py",
        ".md",
        ".yml",
        ".yaml",
        ".toml",
        ".tf",
        ".env.example",
    }

    with open(output_file, "w", encoding="utf-8") as f:
        # Write repository structure
        f.write("=== Repository Structure ===\n\n")
        structure = get_repo_structure()
        f.write("\n".join(structure))
        f.write("\n\n")

        # Write file contents
        f.write("\n=== File Contents ===\n\n")

        for root, _, files in os.walk("."):
            for file in files:
                file_path = pathlib.Path(root) / file

                if not should_include_file(file_path):
                    continue

                if file_path.suffix in important_extensions or file == ".gitignore":
                    f.write(f"\n{'='*80}\n")
                    f.write(f"File: {file_path}\n")
                    f.write(f"{'='*80}\n\n")
                    f.write(read_file_content(file_path))
                    f.write("\n\n")


if __name__ == "__main__":
    save_repo_content()
    print("Repository content has been saved to repository_content.txt")
