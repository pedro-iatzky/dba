"""This script is meant to set Defined roles from scratch.
 Attention, the roles are global for the database cluster and not
  for individual databases"""
import argparse

from sqlalchemy.exc import ProgrammingError

from dbal.database import Database
from .default_roles import Defaults


def execute_or_pass(db, sentence, user=None, raise_exception=False):
    try:
        db.execute(sentence)
        db.commit()
    except ProgrammingError:
        db.rollback()
        print("User: {} is already create in the cluster server".format(user))
        if raise_exception:
            raise ProgrammingError


def create_roles(db, developer_user, developer_pass, viewer_user, viewer_pass):
    execute_or_pass(db, 'CREATE USER "{}" password \'{}\''
                    .format(developer_user, developer_pass), user=developer_user)
    execute_or_pass(db, 'CREATE USER "{}" password \'{}\''
                    .format(viewer_user, viewer_pass), user=viewer_user)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--Viewer_user', type=str, default=Defaults.Viewer_user,
                        help='Set the viewer user name, Default="{}"'
                        .format(Defaults.Viewer_user))
    parser.add_argument('--Viewer_pass', type=str, default=Defaults.Viewer_Pass,
                        help='Set the viewer user password, Default="{}"'
                        .format(Defaults.Viewer_Pass))
    parser.add_argument('--Developer_user', type=str, default=Defaults.Developer_user,
                        help='Set the developer user name, Default="{}"'
                        .format(Defaults.Developer_user))
    parser.add_argument('--Developer_Pass', type=str, default=Defaults.Developer_Pass,
                        help='Set the developer user password, Default="{}"'
                        .format(Defaults.Developer_Pass))
    args = parser.parse_args()

    DB = Database(echo=True)
    # TODO check if the roles already exists, create the roles that do not exist yet.
    create_roles(DB, args.Developer_user, args.Developer_Pass,
                 args.Viewer_user, args.Viewer_pass)
