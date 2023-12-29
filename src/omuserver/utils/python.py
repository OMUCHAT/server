import os
import sys
from typing import List, TypedDict


class LaunchCommand(TypedDict):
    cwd: str
    args: List[str]


def get_launch_command() -> LaunchCommand:
    return {
        "cwd": os.getcwd(),
        "args": [sys.executable, "-m", "omuserver", *sys.argv[1:]],
    }
