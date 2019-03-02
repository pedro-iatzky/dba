"""
This script allows creating the configuration files.
Needed in order to connect to the database in absence of proper environment variables
"""

import argparse
from dbal.Config import Config_db


def parse_environment(env_input):
    if env_input.lower() in ['d', 'dev', 'development']:
        return 'development'
    elif env_input.lower() in ['p', 'prod', 'production']:
        return 'production'
    else:
        raise Exception('Environment not recognized')


def to_dict(db_host, db_name, user, password):
    obj = {'DB_HOST': db_host,
           'DB_NAME': db_name,
           'User': user,
           'Pass': password}
    return obj


def create_file(env, db_host, db_name, user, password):
    config_dict = to_dict(db_host, db_name, user, password)
    conf_file = Config_db.create_config_env_file(config_dict, environment=env)
    return conf_file


def create_config_file():
    env_input = input('Will you be working on a Development or a'
                      ' Production environment(D or P)?\n')
    environment = parse_environment(env_input)

    db_host = input('Input the database host. E.g: 127.0.0.1; '
                    'your_db_host.cpjlg4r7ldnw.us-west-2.rds.amazonaws.com\n')
    db_name = input('Insert the database name.\n')
    user = input('Insert the user name to connect to the database.\n')
    password = input('Insert the user\'s password.\n')
    conf_file = create_file(environment, db_host, db_name, user, password)
    print("Your config file was successfully created in {}. Remember, "
          "you can edit the file manually if you need it.\n".format(conf_file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()

    create_config_file()
