"""
This module aims to contain all the classes and functions needed
 to manipulate the databases.
"""
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from dbal.Config import Config_db

from dbal.schemas.common import Base


def parse_db(db_config):
    _dbparse = "postgresql+psycopg2://{}:{}@{}/{}".format(
        db_config.User, db_config.Pass, db_config.DB_HOST, db_config.DB_NAME
    )
    return _dbparse


@lru_cache(maxsize=10)
def get_default_config_file():
    """
    First, try to get the database connection parameters from the environment_variables.
    If there is no environment variables, get the params from local config files.
    :return:
    """
    try:
        default_config = Config_db.DatabaseConfig.from_environment_variables()
    except Exception:
        try:
            default_config = Config_db.DatabaseConfig.from_json()
        except Exception as raising_exception:
            raise raising_exception
    return default_config


class Singleton(type):
    """
    You have a registry of database instances. Just one by database config objects.
    We assume that the database config object defines uniquely a connection pool
    over a database server. This implementation is not exactly a Singleton pattern,
    it is more like a cache of database objects though. However, the concept is pretty
    much the same.
    """

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # TODO This should be improved. For example, we could have a more consistent
        # registry in the DatabaseConfig class as well
        cls.__registry = dict()

    def __call__(cls, *args, db_config=None, **kwargs):
        if not db_config:
            # Here we instance the config object from the either
            # environment variables or the default config files.
            db_config = get_default_config_file()
        if db_config not in cls.__registry:
            cls.__registry[db_config] = super().__call__(*args, db_config=db_config,
                                                         **kwargs)
        return cls.__registry[db_config]

    """
    If you want to have just one database instance (i.e. a truly Singleton pattern) 
    comment the lines above and uncomment the lines below.
    """
    #
    # def __init__(cls, *args, **kwargs):
    #         super().__init__(*args, **kwargs)
    #         cls._instance = None
    #
    # def __call__(cls, *args, db_config=default_db_config, **kwargs):
    #     print(db_config)
    #     if cls._instance is None:
    #         cls._instance = super().__call__(*args, db_config=db_config, **kwargs)
    #     return cls._instance


class Database(metaclass=Singleton):
    """
    This class is meant to abstract the developer to manage the connections and
    instances of the databases.
    It is encouraged to improve this one.
    The classes of SQLalchemy defined in a declarative way should be more than enough
    to load and retrieve data from the database.
    Caution!! In order to assure this class to be initialized just once and shared over
    all your code, the metaclass Singleton was created. This metaclass assures that just
    one instance of database (by db_config object!!!) is initiated. Successive
    initialization intents just act as callings for the singleton class.
    """

    def __init__(self, autocommit=False, echo=False, multithreading=False,
                 db_config=None, dev_mode=False, pool_size=5):
        """
        Initialize the Database object.
        View Singleton design pattern.
        :param autocommit: <bool>. Set True if you want to activate the orm autocommit.
            Not recommended
        :param echo: <bool>. Set True if you want to echo in screen all the native sql
            commands emitted by SQLalchemy. Useful when debugging
        :param multithreading: <bool>. Set True if you want to use multithreading.
            Careful! It will open one connection for each thread.
        :param db_config: <DatabaseConfig>. The database configuration object.
            This object has the database connection parameters.
            This object is the one that is used to create the database registry
            in the custom Singleton implementation
        :param dev_mode: <bool>. In dev(eloper) mode ALL the changes performed to the
            database are not committed by default when calling the commit method
            (i.e. the transaction is open until the end). However, if you want to
            finish the transaction and persist the database changes you can use
            the "persist_changes" method.
        :param pool_size: <int>. Set the limit to the connections that can be opened
            in the multithreading mode. This could be useful for setting a safety
            upper limit.
        """
        self.db_config = db_config
        self.autocommit = autocommit
        if autocommit:
            self.engine = create_engine(
                parse_db(self.db_config), echo=echo, pool_size=pool_size,
                isolation_level="AUTOCOMMIT"
            )
        else:
            self.engine = create_engine(
                parse_db(self.db_config), echo=echo, pool_size=pool_size
            )
        self.meta = Base.metadata
        self.session_factory = sessionmaker(bind=self.engine)
        self.multithreading = multithreading
        if self.multithreading:
            self.Session = scoped_session(self.session_factory)
        else:
            self.Session = sessionmaker(bind=self.engine, autocommit=autocommit)
        self._session = self.Session()
        self.__conn = None
        self.__cursor = None
        self._bulkops = BulkOps()
        self._dev_mode = dev_mode
        self._echo = echo

    @property
    def session(self):
        """
        If no multithreading is selected the session is shared between all threads.
        The session is local to each thread, that is, the queries for every season
        are independent and isolated from each other.
        :return:
        """
        if self.multithreading:
            return self.Session()
        return self._session

    def write(self, obj, commit=True):
        try:
            self.session.add(obj)
            self.session.flush()
            if commit:
                self.commit()
        except Exception as e:
            self.rollback()
            raise e

    def read(self, table, *args, limit=100, last=False):
        """
        :param table: <sqlalchemy.ext.declarative.api.DeclarativeMeta>
        :param args: <sqlalchemy.orm.attributes.InstrumentedAttribute>.
            E.g: PipeRun.run_id == 5
        :param last: <boolean> Set true if you want to get the last inserted records
        :param limit: <int>
        :return:
        """
        if last:
            pkey = self.get_primary_key(table)
            query = self.session.query(table).filter(*args). \
                order_by(table.__getattribute__(table, pkey).desc())
        else:
            query = self.session.query(table).filter(*args)
        if limit:
            return query.limit(limit).all()
        return query.all()

    def read_one(self, table, *args):
        query = self.session.query(table).filter(*args)
        return query.one()

    def update(self, commit=True):
        try:
            self.session.flush()
            if commit:
                self.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def commit(self):
        if not self.autocommit:
            if self.session and not self._dev_mode:
                try:
                    self.session.commit()
                except Exception as e:
                    self.session.rollback()
                    raise e

    def rollback(self):
        if self.session:
            self.session.rollback()

    def execute(self, query):
        """
        Execute a custom query and return an iterable sqlalchemy ResultProxy
        :param query: <str>
        :return: <sqlalchemy.engine.result.ResultProxy>
        """
        try:
            return self.session.execute(query)
        except Exception as e:
            self.session.rollback()
            raise e

    @staticmethod
    def get_primary_key(base_obj):
        """
        Get the primary key for a table
        :param base_obj: <sqlalchemy.ext.declarative.api.DeclarativeMeta>
        :return:
        """
        return base_obj.__table__.primary_key.columns.values()[0].key

    @staticmethod
    def check_consistency(inst_type, *args):
        for arg in args:
            if not isinstance(arg, inst_type):
                raise TypeError('The argument {} is not from the'
                                ' correct type'.format(arg))
            else:
                pass

    @property
    def cursor(self):
        """
        Init the connection if it is not already initialized.
        Attention, every time you request a cursor, it is a different one
         (within the same transaction, however)
        :return:
        """
        __conn = self.session.connection().connection
        return __conn.cursor()

    def insert_many(self, *args, **kwargs):
        try:
            return self._bulkops.insert_many(
                self.cursor, *args, echo=self._echo, **kwargs
            )
        except Exception as e:
            self.rollback()
            raise e

    def persist_changes(self):
        """
        This method is meant to persist the database changes if you are in dev_mode
        :return:
        """
        # TODO, this is a bad implementation. Surely this can be improved or, at least,
        #  be more comprehensible
        self._dev_mode = False
        self.commit()
        self._dev_mode = True

    def close_session(self):
        if self.multithreading:
            self.Session.remove()
        else:
            self.session.close()

    def update_many(self, *args, **kwargs):
        try:
            return self._bulkops.update_many(self.cursor, *args,
                                             echo=self._echo, **kwargs)
        except Exception as e:
            self.rollback()
            raise e


class BulkOps(object):

    def query(self, table, fields_str, args_str, sub_query, to_return):
        main = """
                INSERT INTO {} ({})
                VALUES {}""".format(table, fields_str, args_str)
        if to_return:
            to_ret = ', '.join(to_return)
            ret_query = """ RETURNING {}""".format(to_ret)
        else:
            ret_query = ''
        if sub_query and args_str is None:
            main = """INSERT INTO {} ({}) """.format(table, fields_str)
            condition = sub_query
        else:
            condition = ''
        _query = main + condition + ret_query
        return _query

    def insert_many(self, cursor, table, columns, values,
                    sub_query=None, to_return=None, echo=False):
        """
        This function allows for inserting many values in bulk.
        This function was created because the sqlalchemy bulk inserts were not
        fast enough
        :param cursor: <psycopg2.cursor> or something like that
        :param table: <str> table name
        :param columns: <tuple>. columns to insert
        :param values: <list>.<tuple>. Values to bulk insert. The positions must
            be consistent with the positions of each column name in the columns array
        :param sub_query: <str>. Optional subquery. Use with caution
        :param to_return: <tuple>. values to return. Usually it is useful to return
            primary or foreign keys
        :param echo: <bool>. If true, print the commands that are being executed
        :return:
        """
        # TODO It would be great to used the sqlachemy declarative objects directly
        #  instead of passing all this overhead (table, columns, values) separately
        fields_str = ', '.join(columns)
        # get values, ordered by field positions

        # this is for transforming values into string.
        gen_tuple = '(' + ', '.join(['%s' for _ in iter(columns)]) + ')'

        args_str = ','.join(cursor.mogrify(gen_tuple, x).decode('utf-8')
                            for x in values) if values else None

        _query = self.query(table, fields_str, args_str, sub_query, to_return)
        if echo:
            print('Inserting values in table {}'.format(table))
        try:
            cursor.execute(_query)
            if to_return:
                c = cursor.fetchall()
            else:
                c = None
            return c
        except Exception as e:
            raise e

    def _create_temp_table_from_existent(self, cursor, table, columns=None,
                                         schema_only=True):
        """

        :param table: <str>. The table you want to create the temporary one from
        :param columns: <tuple>. The tuple with the column's names to use for
            creating the table. If None, all columns will be used
        :param schema_only: <bool>. True if you want the table to have only the schema,
            i.e. not the data
        :return:
        """
        fields_str = ', '.join(columns) if columns else '*'
        data_filter = 'WITH NO DATA' if schema_only else ''
        temp_table_name = 'temp_ud_table'
        query = """
        CREATE TEMP TABLE {}
        ON COMMIT DROP
        AS SELECT {}
        FROM {} {}
        """.format(temp_table_name, fields_str, table, data_filter)
        cursor.execute(query)
        return temp_table_name

    def update_many(self, cursor, table, prim_key_columns, values, echo=False):
        """
        Update many values for an existing table. AKA bulk update.
        :param cursor: <psycopg2.cursor>
        :param table: <str> the table name
        :param prim_key_columns: <tpl>. Where the first value must be the primary key !!
        :param values: <list>.<tuple>. Values to bulk insert.
            The positions must be consistent with the positions of each column name
            in the prim_key_columns array
        :param echo: <bool>. Set True if you want to be more verbose
        :return:
        """
        # First we create a temporal table using the schema from the original one
        if echo:
            print('creating the temporal table')
        temp_table = self._create_temp_table_from_existent(
            cursor, table, columns=prim_key_columns, schema_only=True
        )
        # Then, we populate the temporal table with the values to update
        if echo:
            print('Inserting the values to update in the temporal table')
        self.insert_many(cursor, temp_table, prim_key_columns, values)
        # Finally, we update the table with the new values, using a join with the
        #  temporal one, for being more efficient
        temp_alias = 'temp'
        prim_key = prim_key_columns[0]
        columns_to_ud = prim_key_columns[1:]
        set_str = ', '.join(map(lambda c: "{} = {}.{}".format(c, temp_alias, c),
                                columns_to_ud))
        join_filter = '{}.{} = {}.{}'.format(table, prim_key, temp_alias, prim_key)
        ud_query = """
        UPDATE {}
        SET {}
        FROM {} {} WHERE {}
        """.format(table, set_str, temp_table, temp_alias, join_filter)
        if echo:
            print('Updating the table with the specified values')
        cursor.execute(ud_query)
