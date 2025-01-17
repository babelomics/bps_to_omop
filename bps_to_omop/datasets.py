from os import getenv

from dotenv import find_dotenv, load_dotenv

"""
Data directory path
"""

find_dotenv()
load_dotenv()

path = getenv("LOCAL_DATA_DIR")

data_dir = path
