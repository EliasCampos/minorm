import os


DATABASE_DRIVER_PARAM = 'DATABASE_DRIVER'
DATABASE_URL_PARAM = 'DATABASE_URL'

_default_db = None


def get_default_db():
    global _default_db
    if _default_db:
        return _default_db

    driver_name = os.getenv(DATABASE_URL_PARAM)
    driver_mapping = {
        'sqlite': SQLiteDatabase,
    }

    db_class = driver_mapping[driver_name]
    connection_string = read_connection_string()
    _default_db = db_class(connection_string)
    _default_db.connect()
    return _default_db


class Database:

    def __init__(self, connection_string):
        self.connection_string = connection_string

        self.last_query_rowcount = 0
        self.last_query_lastrowid = None

        self._connection = None

    def connect(self):
        self.disconnect()

        driver = self.get_driver()
        self._connection = driver.connect(self.connection_string)

    def disconnect(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute(self, raw_sql, params, fetch=False):
        if not self._connection:
            return None

        with self._connection:
            with self._connection.cursor() as cur:
                cur.execute(raw_sql, params)

                self.last_query_rowcount = cur.rowcount
                self.last_query_lastrowid = cur.lastrowid

                return cur.fetchall() if fetch else None

    def get_driver(self):
        raise NotImplementedError


class SQLiteDatabase(Database):

    def get_driver(self):
        import sqlite3
        return sqlite3


def read_connection_string():
    return os.getenv(DATABASE_URL_PARAM)
