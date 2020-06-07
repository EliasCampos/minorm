import pytest

from minorm.db import SQLiteDatabase


@pytest.fixture(scope="function")
def test_db():
    db = SQLiteDatabase(":memory:")
    db.connect()
    yield db
    db.disconnect()
