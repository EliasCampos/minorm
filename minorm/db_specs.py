from decimal import Decimal


class BaseSpec:
    """A base class for DB wrapper, to provide common interface for different database implementations."""

    VALUE_ESCAPE = None  # a character which is used as placeholder for sql parameter, to avoid sql injections
    AUTO_FIELD_TYPE = None  # an sql base type name for auto incremented field
    AUTO_FIELD_CONSTRAINS = ()  # constrains that auto incremented field should have (ex. 'AUTOINCREMENT')

    def __init__(self, connection_url):
        assert self.VALUE_ESCAPE, f"{self.__class__.__name__} should define value escape."
        assert self.AUTO_FIELD_TYPE,  f"{self.__class__.__name__} should define auto field type."

        self.connection_url = connection_url
        self.db_driver = self.prepare_db_driver()

    def prepare_db_driver(self):
        """
        Should return a module that implements Python database API interface.

        https://www.python.org/dev/peps/pep-0249/#module-interface
        """
        raise NotImplementedError

    def set_autocommit(self, connection, autocommit):
        """The method should manipulate on connection object to turn on/off auto-commit mode."""
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
        import sqlite3  # pylint: disable=import-outside-toplevel

        sqlite3.register_adapter(Decimal, str)
        sqlite3.register_converter("DECIMAL", Decimal)

        return sqlite3

    def create_connection(self):
        sqlite3 = self.db_driver
        connection = sqlite3.connect(
            self.connection_url,
            detect_types=sqlite3.PARSE_DECLTYPES,  # parse declared types and convert to a proper python value
        )
        return connection

    def set_autocommit(self, connection, autocommit):
        connection.isolation_level = None if autocommit else ''


class PostgreSQLSpec(BaseSpec):
    VALUE_ESCAPE = '%s'
    AUTO_FIELD_TYPE = "SERIAL"

    def prepare_db_driver(self):
        try:
            import psycopg2  # pylint: disable=import-outside-toplevel
        except ImportError as err:
            raise RuntimeError(f"{self.__class__.__name__} requires psycopg2 to be installed.") from err
        return psycopg2

    def set_autocommit(self, connection, autocommit):
        connection.autocommit = autocommit
