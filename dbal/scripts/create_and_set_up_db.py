"""
This script allows creating a database and the schemas from scratch.
It populates the "constants" database values as well.
"""
import argparse
from dbal.Config.Set_db import main as database_schema_creation
from dbal.database import Database
from dbal.Config.Config_db import DatabaseConfig

__debug = True  # Set true for printing in screen which commands
# are being issued to the database


def create_db(db_obj, database, template=None):
    # First, we create the database with an existent one.
    # If the database is not already created
    print('Checking the database is not already created')
    current_dbs = [db[0] for db in db_obj.execute(
        "SELECT datname FROM pg_database WHERE datistemplate = false"
    )]
    if database in current_dbs:
        print("The database was already created.")
    else:
        stmt = "CREATE DATABASE {}".format(database)
        if template:
            stmt += " TEMPLATE {}".format(template)
            # we need to terminate all the sessions with the template in order
            # to execute a the copy.
            # TODO can I go on doing this if I terminate my connection as well?
            db_obj.execute('select pg_terminate_backend(pid) '
                           'from pg_stat_activity where datname = \'{}\''
                           .format(template))
        db_obj.execute(stmt)
        print("The database was created successfully")


def _parse_new_config(old_config_dict, new_db):
    config = {k: v for k, v in old_config_dict.items() if k != 'DB_NAME'}
    config['DB_NAME'] = new_db
    new_db_config = DatabaseConfig(config)
    return Database(db_config=new_db_config, echo=__debug)


def main(db_name, db_src):
    config_db = Database(echo=__debug, autocommit=True)
    # create the database
    create_db(config_db, db_name, template=db_src)
    if db_src:
        # If template, we assume that there is no need of create the schema
        # and the constants
        return
    new_db_obj = _parse_new_config(config_db.db_config.config, db_name)
    # Create the schema
    print("Creating the database schema")
    database_schema_creation(new_db_obj)
    print("Database schema created successfully")
    print("Constants were properly populated into the database")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('db_name', metavar="Database Name", type=str,
                        help="The name of the database to be created")
    parser.add_argument('--db_template', metavar="Database Name", type=str, default=None,
                        help="The name of the template database, i.e. "
                             "the database you will be replicating.")
    args = parser.parse_args()
    main(args.db_name, args.db_template)
