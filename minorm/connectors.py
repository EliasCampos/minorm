from contextlib import contextmanager


class ConnectorError(RuntimeError):
    pass


class Connector:
    NOT_CONNECTED_ERROR = 'Connect was not performed.'

    def __init__(self):
        self._connection = None
        self._db_spec = None
        self._autocommit = False

    def connect(self, db_spec):
        self.disconnect()

        self._connection = db_spec.create_connection()
        self._db_spec = db_spec
        # Python DB API requires autocommit to be turn initially off
        # but it's not matches SQL standard,
        # so it's preferable to change the default behaviour:
        self.set_autocommit(True)

        return self

    def disconnect(self):
        if self._connection:
            self._connection.close()
            self._connection = None

        self._db_spec = None
        self._autocommit = None

    def set_autocommit(self, autocommit):
        self.spec.set_autocommit(self.connection, autocommit)
        self._autocommit = autocommit

    @property
    def spec(self):
        self._check_if_connected()
        return self._db_spec

    @property
    def connection(self):
        """Return current connection. Will raise exception if no connection was performed."""
        self._check_if_connected()
        return self._connection

    @contextmanager
    def cursor(self):
        yield self.connection.cursor()

    def _check_if_connected(self):
        if not self._connection:
            raise ConnectorError(self.NOT_CONNECTED_ERROR)


connector = Connector()
