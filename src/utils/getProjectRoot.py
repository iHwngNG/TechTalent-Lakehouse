import os
import sys
from dotenv import load_dotenv, find_dotenv
from pathlib import Path


def getRootPath():
    load_dotenv(find_dotenv())
    PROJECT_ROOT = os.environ.get("PROJECT_ROOT") or str(
        Path(__file__).resolve().parent.parent
    )
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)

    return PROJECT_ROOT
