"""
WARNING: THIS SCRIPT WILL DELETE ALL DATA, INCLUDING DATA/KAFKA, DATA/POSTGRES, DATA/OBSERVATIONS
"""

import os
import shutil
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()

def clear_data():
    """Clear all data from the data directory."""
    data_dir = Path(os.getenv('DATA_PATH'))/'kafka'
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
        print(f"Deleted {data_dir}")
    else:
        print(f"{data_dir} does not exist")
    data_dir = Path(os.getenv('DATA_PATH'))/'postgres'
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
        print(f"Deleted {data_dir}")
    else:
        print(f"{data_dir} does not exist")
    data_dir = Path(os.getenv('DATA_PATH'))/'pgadmin'
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
        print(f"Deleted {data_dir}")
    else:
        print(f"{data_dir} does not exist")

if __name__ == "__main__":
    print("WARNING: THIS SCRIPT WILL DELETE ALL DATA, INCLUDING DATA/KAFKA, DATA/POSTGRES, DATA/OBSERVATIONS")
    input("Continue, press Enter, or Ctrl + C to stop!")
    os.system("docker compose down")
    clear_data()