import sqlite3

from minorm.specs import SQLiteSpec


class TestSQLiteSpec:

    def test_prepare_db_driver(self):
        db_spec = SQLiteSpec("")

        assert db_spec.db_driver is sqlite3

    def test_create_connection(self):
        db_spec = SQLiteSpec(":memory:")
        conn = db_spec.create_connection()
        assert isinstance(conn, sqlite3.Connection)
        assert conn.row_factory is sqlite3.Row

    def test_value_escape(self):
        db_spec = SQLiteSpec("")
        assert db_spec.value_escape == '?'

    def test_auto_field_type(self):
        db_spec = SQLiteSpec("")
        assert db_spec.auto_field_type == 'INTEGER'

    def test_auto_field_constrains(self):
        db_spec = SQLiteSpec("")
        assert db_spec.auto_field_constrains == ('AUTOINCREMENT',)
