"""
This submodule is meant to manage (retrieve, parse and create) the database
parameters that then will be used to connect to the database
"""
import os
import json
from pkg_resources import resource_filename

Environment_config = resource_filename(__name__, 'Environment_config.json')


class ConfigFileNotCreated(FileNotFoundError):
    def __init__(self):
        super().__init__('You must create the configuration file prior connecting'
                         ' to the database. '
                         'You may use the "create_config_file" script.')


def config_dir():
    """
    Get the config local directory. It is currently set as ~/.config/dbal
    :return:
    """
    home = os.path.expanduser("~")
    config_suffix = os.path.join('.config', 'dbal')
    directory = os.path.join(home, config_suffix)

    if not os.path.exists(directory):
        os.makedirs(directory)

    return directory


def __get_path(file_name, module_config):
    if not module_config:
        directory = config_dir()
        prod_config_file = os.path.join(directory, file_name)
    else:
        prod_config_file = resource_filename(__name__, file_name)
    return prod_config_file


def environment_config_file_path(module_config=False):
    """
    Get the environment configuration file path
    :param module_config: <bool>. If False it will look for the file at
        the ~/.config/dbal/ folder. If True it will look for the file inside
        the module (not recommended)
    :return:
    """
    name_env_config = 'environment_config.json'
    return __get_path(name_env_config, module_config)


def create_config_env_file(credentials_dict, environment='development'):
    environment = environment.lower()
    if environment not in ['development', 'production']:
        raise ValueError('Environment {} not allowed'.format(environment))
    obj = {
        "default": environment,
        "environments": {
            environment: {
                "DB_HOST": credentials_dict.get("DB_HOST"),
                "DB_NAME": credentials_dict.get("DB_NAME"),
                "User": credentials_dict.get("User"),
                "Pass": credentials_dict.get("Pass")
            }
        }
    }
    env_path = environment_config_file_path()
    with open(env_path, 'w') as env_file:
        json.dump(obj, env_file, indent=4)
    return env_path


def get_config_dict(environment=None):
    """
    Retrieve the default environment according to a configuration file
    :return:
    """
    env_config_file = environment_config_file_path()
    if not os.path.exists(env_config_file):
        raise ConfigFileNotCreated
    else:
        with open(env_config_file) as file:
            env_obj = json.load(file)
            if not environment:
                environment = env_obj.get('default')
        return env_obj.get('environments', {}).get(environment)


class DatabaseConfig(object):
    """
    This class contains the 4 required parameters for connecting to a database
    """
    def __init__(self, config_dict):
        """

        :param config_dict: <dict> Must have the format:
        {
          "DB_HOST": "127.0.0.1",
          "DB_NAME": "your_db",
          "User": "your_user",
          "Pass": "your_pass"
        }
        """

        self.config = config_dict
        self.DB_HOST = self.config['DB_HOST']
        self.DB_NAME = self.config['DB_NAME']
        self.User = self.config['User']
        self.Pass = self.config['Pass']

    @classmethod
    def from_json(cls, environment=None):
        """
        Instantiate the object using configuration file
        :return: <cls>
        """
        config = get_config_dict(environment=environment)
        return cls(config)

    @classmethod
    def from_environment_variables(cls, variables_encrypted=False):
        """
        We assume that the variables are encrypted using a AWS CMK key
        :param variables_encrypted: bool. True if (all) the environment variables
        are encrypted.
        :return: <cls>
        """
        try:
            config_encrypted_object = dict()
            config_encrypted_object['DB_HOST'] = os.environ['DATABASE_HOST']
            config_encrypted_object['DB_NAME'] = os.environ['DATABASE_NAME']
            config_encrypted_object['User'] = os.environ['DATABASE_USER']
            config_encrypted_object['Pass'] = os.environ['DATABASE_PASSWORD']
            if variables_encrypted is True:
                raise NotImplementedError("You cannot encrypt the environment"
                                          " variables so far")
            else:
                config_decrypted_object = config_encrypted_object

            return cls(config_decrypted_object)
        except Exception as e:
            raise e

    def get_parameters_encrypted(self):
        """
        Return the database parameters in a dictionary format,
        with the parameters encrypted and encoded in Base64
        This method leverages AWS kms encryption service
        :return: <dict>
        """
        raise NotImplementedError()
        # plain_dict = {
        #     'DATABASE_HOST': self.DB_HOST,
        #     'DATABASE_NAME': self.DB_NAME,
        #     'DATABASE_USER': self.User,
        #     'DATABASE_PASSWORD': self.Pass
        # }
        # encrypted_dict = kms.encrypt_dictionary(plain_dict)
        # return encrypted_dict
