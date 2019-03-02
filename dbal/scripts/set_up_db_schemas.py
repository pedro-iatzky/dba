"""
This script allows for creating the database schema from scratch
"""
import argparse
from dbal.Config.Set_db import main
from dbal.database import Database

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    db = Database()
    main(db)
    print('All schemas were correctly populated into the database')
