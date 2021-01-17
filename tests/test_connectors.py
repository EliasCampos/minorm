import pytest

from minorm.connectors import Connector, ConnectorError
from minorm.db_specs import SQLiteSpec


class TestConnector:

    def test_connect(self):
        connector = Connector()

        db_spec = SQLiteSpec(":memory:")
        result = connector.connect(db_spec)
        assert result is connector
        assert connector._connection
        assert connector._db_spec is db_spec
        assert connector._autocommit

    def test_spec(self):
        db_spec = SQLiteSpec(":memory:")
        connector = Connector().connect(db_spec)
        assert isinstance(connector.spec, SQLiteSpec)
        assert connector.spec is db_spec

    def test_spec_not_connected(self):
        connector = Connector()
        with pytest.raises(ConnectorError, match=r'[cC]onnect.*'):
            connector.spec

    @pytest.mark.parametrize('autocommit', [True, False])
    def test_set_autocommit(self, test_db, autocommit):
        test_db.set_autocommit(autocommit)
        assert test_db._autocommit == autocommit

    def test_set_autocommit_not_connected(self):
        connector = Connector()
        with pytest.raises(ConnectorError, match=r'[cC]onnect.*'):
            connector.set_autocommit(True)

    def test_cursor_not_connected(self):
        connector = Connector()
        with pytest.raises(ConnectorError, match=r'[cC]onnect.*'):
            with connector.cursor():
                pass
