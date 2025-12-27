import os
from pathlib import Path

# Base directory of the project
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Data directory
DATA_DIR = BASE_DIR / "data"
DAILY_DATA_DIR = DATA_DIR / "daily"
MIN5_DATA_DIR = DATA_DIR / "min5"
DB_PATH = DATA_DIR / "meta.db"

# Ensure essential directories exist
def init_directories():
    DATA_DIR.mkdir(exist_ok=True, parents=True)
    DAILY_DATA_DIR.mkdir(exist_ok=True, parents=True)
    MIN5_DATA_DIR.mkdir(exist_ok=True, parents=True)


# Call on import to ensure they exist
init_directories()
