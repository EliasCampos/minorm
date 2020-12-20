from decimal import Decimal


class BaseSpec:
    VALUE_ESCAPE = None
    AUTO_FIELD_TYPE = None
    AUTO_FIELD_CONSTRAINS = ()

    def __init__(self, connection_url):
        assert self.VALUE_ESCAPE, f"{self.__class__.__name__} should define value escape."
        assert self.AUTO_FIELD_TYPE,  f"{self.__class__.__name__} should define auto field type."

        self.connection_url = connection_url  # TODO: add validation for certain database

        self.db_driver = self.prepare_db_driver()

    def prepare_db_driver(self):
        raise NotImplementedError

    def create_connection(self):
        connection = self.db_driver.connect(self.connection_url)
        return connection

    @property
    def value_escape(self):
        return str(self.VALUE_ESCAPE)

    @property
    def auto_field_type(self):
        return str(self.AUTO_FIELD_TYPE)

    @property
    def auto_field_constrains(self):
        return tuple(self.AUTO_FIELD_CONSTRAINS)


class SQLiteSpec(BaseSpec):
    VALUE_ESCAPE = '?'
    AUTO_FIELD_TYPE = "INTEGER"
    AUTO_FIELD_CONSTRAINS = ("AUTOINCREMENT",)

    def prepare_db_driver(self):
        import sqlite3

        sqlite3.register_adapter(Decimal, str)
        sqlite3.register_converter("DECIMAL", Decimal)

        return sqlite3


class PostgreSQLSpec(BaseSpec):
    VALUE_ESCAPE = '%s'
    AUTO_FIELD_TYPE = "SERIAL"
    HAS_AUTO_FIELD_AUTO_INCREMENT = False

    def prepare_db_driver(self):
        import psycopg2
        return psycopg2
