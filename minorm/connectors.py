from contextlib import contextmanager


class ConnectorError(RuntimeError):
    pass


class Connector:
    NOT_CONNECTED_ERROR = 'Connect was not performed.'

    def __init__(self):
        self._connection = None
        self._db_spec = None

    def connect(self, db_spec):
        self.disconnect()

        self._db_spec = db_spec
        self._connection = self.spec.create_connection()

        return self

    def disconnect(self):
        if self._connection:
            self._connection.close()
            self._connection = None
        self._db_spec = None

    @property
    def spec(self):
        if not self._db_spec:
            raise ConnectorError(self.NOT_CONNECTED_ERROR)

        return self._db_spec

    @contextmanager
    def cursor(self):
        if not self._connection:
            raise ConnectorError(self.NOT_CONNECTED_ERROR)

        with self._connection:
            yield self._connection.cursor()


connector = Connector()
