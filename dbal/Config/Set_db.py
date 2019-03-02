"""
This script allows creating the database schema from scratch.
Using the SQLalchemy declarative schema classes
"""
from dbal.database import Database
from dbal.schemas.common import Base
from os import chdir

from dbal import __file__ as package_path
# from alembic.config import Config
# from alembic import command

import argparse


class VersionedDatabaseException(Exception):
    def __init__(self):
        super().__init__(
            "You are working with an already versioned database."
            " Use alembic cvs in order to update the database "
            "schema to its last version.")


def drop_table(engine, *args, All=None):
    """
    Drop tables from the db
    :param All: set True if you want to delete all tables. Use with caution!!!
    :param args: Set the object table classes you want to drop.
    :return:
    """
    meta = Base.metadata

    if All is None:
        All = False
    try:
        if All:
            meta.drop_all(engine)
        else:
            for table in args:
                table.__table__.drop(engine)

        return None
    except Exception as e:
        raise e


def create_table(engine, *args, All=None):
    """
    Create tables in the db
    :param All: set True if you want to delete all tables. Use with caution!!!
    :param args: Set the object table classes you want to drop.
    :return:
    """
    meta = Base.metadata
    if All is None:
        All = False
    try:
        if All:
            meta.create_all(engine)
        else:
            for table in args:
                table.__table__.create(engine)

        return None
    except Exception as e:
        raise e


def stamp_alembic_head():
    module_path = package_path.split('/')[:-1]
    alembic_path = package_path.split('/')[:-1]
    alembic_ini = 'alembic.ini'
    alembic_path.append(alembic_ini)
    # alembic_cfg = Config('/'.join(alembic_path))
    # # If we do not do this, the script fails, because alembic is in the module folder
    # chdir('/'.join(module_path))
    # command.stamp(alembic_cfg, "head")


def check_if_versioned_db(database):
    alembic_version = database.execute(
        """select * from information_schema.tables
           WHERE  table_schema = 'public'
           AND "table_name" = 'alembic_version'""")
    alembic_version = [a_b for a_b in alembic_version]
    if alembic_version:
        raise VersionedDatabaseException


def main(database_obj):
    """
    Create all schemas according to the declarative schema classes,
    only if there is no tables created yet
    :return:
    """

    # Create all tables if there is no one created yet
    # To implement with alembic
    # check_if_versioned_db(database_obj)
    engine = database_obj.engine
    create_table(engine, All=True)
    # Stamp the last version of the database control version
    # To implement with alembic
    # stamp_alembic_head()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()

    db = Database()
    main(db)
