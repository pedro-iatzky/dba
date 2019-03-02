"""This script is meant to assign the roles for the database defined in the config file.
 Attention, the roles are global for the database cluster"""
from dbal.database import Database
import argparse
from sqlalchemy.exc import ProgrammingError
from .default_roles import Defaults
from .helpers import Sentences


def execute_or_pass(db, sentence, raise_exception=False):
    try:
        db.execute(sentence)
        db.commit()
    except ProgrammingError:
        db.rollback()
        if raise_exception:
            raise ProgrammingError


def assign_roles(db, developer_user, viewer_user):

    execute_or_pass(db, 'GRANT SELECT, UPDATE, INSERT, DELETE ON ALL TABLES IN'
                        ' SCHEMA public TO \"{}\"'.format(developer_user))
    # Grant access to future tables
    execute_or_pass(db, 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT,'
                        ' UPDATE, DELETE ON TABLES TO "{}"'.format(developer_user))
    execute_or_pass(db, Sentences.grant_permissions_on_all_sequences(developer_user))
    execute_or_pass(db, Sentences.alter_def_permissions_on_all_sequences(developer_user))

    execute_or_pass(db, 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{}"'
                    .format(viewer_user))
    # Grant access to future tables
    execute_or_pass(db, 'ALTER DEFAULT PRIVILEGES IN SCHEMA public'
                    ' GRANT SELECT ON TABLES TO "{}"'.format(viewer_user))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--Viewer_user', type=str, default=Defaults.Viewer_user,
                        help='Set the viewer user name, Default="{}"'
                        .format(Defaults.Viewer_user))
    parser.add_argument('--Developer_user', type=str, default=Defaults.Developer_user,
                        help='Set the developer user name, Default="{}"'
                        .format(Defaults.Developer_user))
    args = parser.parse_args()

    DB = Database()

    assign_roles(DB, args.Developer_user, args.Viewer_user)
