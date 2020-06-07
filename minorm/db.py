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


class DBDriver:
    SQLITE = 'sqlite3'
    POSTGRES = 'psycopg2'


class Database:
    DRIVER = None
    VAL_PLACE = None

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

    def execute(self, raw_sql, params=(), fetch=False, many=False):
        if not self._connection:
            return None

        params = tuple(params)
        with self._connection:
            cur = self._connection.cursor()
            if many:
                cur.executemany(raw_sql, params)
            else:
                cur.execute(raw_sql, params)

            self.last_query_rowcount = cur.rowcount
            self.last_query_lastrowid = cur.lastrowid

            return cur.fetchall() if fetch else None

    def last_insert_row_id(self):
        return self.last_query_lastrowid

    def get_driver(self):
        driver = __import__(self.DRIVER)
        return driver


class SQLiteDatabase(Database):
    DRIVER = DBDriver.SQLITE
    VAL_PLACE = '?'


def read_connection_string():
    return os.getenv(DATABASE_URL_PARAM)
