import pytest

from minorm.db import SQLiteDatabase
from minorm.fields import CharField, IntegerField
from minorm.models import Model


@pytest.fixture(scope="function")
def test_db():
    db = SQLiteDatabase(":memory:")
    db.connect()
    yield db
    db.disconnect()


@pytest.fixture(scope="function")
def test_model(test_db):
    class Person(Model):
        name = CharField(max_length=255)
        age = IntegerField()

        class Meta:
            db = test_db

    Person.create_table()
    yield Person
    Person.drop_table()
