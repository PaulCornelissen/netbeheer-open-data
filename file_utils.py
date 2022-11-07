"""
Utils for loading local files
"""
from typing import Optional
import os


def path_from(folder_name: str, file_name: Optional[str]) -> str:
    """
    Turns a folder name and file name into a path to that file
    """
    return os.path.join(folder_name, file_name) if file_name else folder_name


def load_text_file(folder_name: str, file_name: Optional[str]) -> str:
    """
    Load a text file from a folder and optional filename.
    """

    local_path: str = path_from(folder_name, file_name)

    with open(local_path, "r", encoding="utf-8") as file:
        content = file.read()
    return content
