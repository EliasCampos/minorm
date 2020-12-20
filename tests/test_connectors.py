import pytest

from minorm.connectors import Connector, ConnectorError
from minorm.specs import SQLiteSpec


class TestConnector:

    def test_connect(self):
        connector = Connector()

        db_spec = SQLiteSpec(":memory:")
        result = connector.connect(db_spec)
        assert result is connector
        assert connector._connection
        assert connector._db_spec is db_spec

    def test_spec(self):
        db_spec = SQLiteSpec(":memory:")
        connector = Connector().connect(db_spec)
        assert isinstance(connector.spec, SQLiteSpec)
        assert connector.spec is db_spec

    def test_spec_not_connected(self):
        connector = Connector()
        with pytest.raises(ConnectorError, match=r'[cC]onnect.*'):
            connector.spec

    def test_cursor_not_connected(self):
        connector = Connector()
        with pytest.raises(ConnectorError, match=r'[cC]onnect.*'):
            with connector.cursor():
                pass
