"""This script is meant to update existing roles"""
import argparse
from .helpers import Sentences, execute_or_fail


def update_password(db, role_name, new_password):
    sentence = Sentences.change_password(role_name, new_password)
    execute_or_fail(db, sentence)


if __name__ == "__main__":
    from dbal.database import Database
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('role_name', type=str, help='Set the role (or user) name')
    parser.add_argument('password', type=str, help='Set the new role\'s password')

    args = parser.parse_args()

    DB = Database(echo=True)
    update_password(DB, args.role_name, args.password)
    print("Password successfully updated")
