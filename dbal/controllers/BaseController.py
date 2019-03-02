import json
import uuid as _uuid
from abc import ABCMeta, abstractmethod


def serialize(_value, _type):
    """
    Serialize an object to a postgres data type, according to the specified type
    :param _value: the value to cast
    :param _type: the final object cast type
    :return:
    """
    if _value is None:
        return None
    elif _type == 'int':
        return int(_value)
    elif _type == 'varchar':
        return str(_value)
    elif _type == 'jsonb':
        return json.dumps(_value)
    elif _type == 'uuid':
        val = _value if isinstance(_value, _uuid.UUID) else _uuid.UUID(_value)
        return val
    else:
        raise Exception('data type non casteable')


class NoObjectError(Exception):
    pass


class PrimaryKeyIsSerial(Exception):
    def __init__(self):
        super().__init__("You should not set the primary_key id when"
                         " it is an auto_incremented. ")


class PrimaryKeyManuallyInitialized(Exception):

    def __init__(self):
        error_message = """You should never set the primary_key id. the class 
        itself is going to manage the initialization of the key by itself."""
        super().__init__(error_message)


class NotCorrectReference(Exception):
    """Raise this error when the referenced object is not the correct one."""
    pass


class NoDatabase(Exception):
    """Raise this error when the database is not the correct one."""
    pass


class BaseController(metaclass=ABCMeta):
    """
    In this class you must never instantiate directly the object. If you want to write
    an object into the database use the "new" classmethod, if you want to retrieve it,
    use the "from_id" classmethod. Each subclass may contain some other human
    friendly retrieving classmethods, like "from_name", "from_date" and so on.
    """
    def __init__(self, db, _base_model=None):
        """
        :param db: <Database>
        :param _base_model: <Base Model>. You can access to the model attribute
         using the "model" property
        """
        self._db = db
        self.model = _base_model

    @classmethod
    @abstractmethod
    def new(cls, *args, **kwargs):
        """
        Create a new object and save it in the database. If you want
        to retrieve an object that is already in the database use class
        methods from_id or some other that could be available.
        :return:
        """
        pass

    @classmethod
    @abstractmethod
    def from_id(cls, db, base_cls_id):
        """
        Retrieve an object from its id. This object is already present in the database
        :param db: <Database>
        :param base_cls_id: <int> or <uuid> Database id AKA Primary Key
        :return: self.cls
        """
        pass

    @property
    @abstractmethod
    def id(self):
        pass


class WriteableObject(metaclass=ABCMeta):
    """
    In this class you should first initialize the object, and if you want to
    persist the object in the database, use the write method.
    """
    def __init__(self, db, **kwargs):
        """

        :param db: <Database>
        """
        self._db = db
        self._id = kwargs['_id'] if '_id' in kwargs else None
        self._model = kwargs['_model'] if '_model' in kwargs else None

    @abstractmethod
    def write(self, *args, **kwargs):
        """
        Writes the object in the database
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @classmethod
    @abstractmethod
    def from_model(cls, *args, **kwargs):
        """
        This class method is useful when instantiating a controller
         from its database schema object (AKA declarative base)
        :param args:
        :param kwargs:
        :return:
        """
        pass

    @property
    def id(self):
        if self._id:
            return self._id
