"""
Utility script to acess LOCAL_DATA_DIR environmental variable
"""

from os import getenv
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

find_dotenv()
load_dotenv()

path = getenv("LOCAL_DATA_DIR")

data_dir = Path(path)
