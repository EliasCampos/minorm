import pytest

from minorm.db import SQLiteDatabase
from minorm.fields import CharField, IntegerField, ForeignKey
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


@pytest.fixture(scope="function")
def related_models(test_model):
    class Book(Model):
        title = CharField(max_length=120)
        author = ForeignKey(to=test_model, on_delete=ForeignKey.CASCADE)

        class Meta:
            db = test_model.db

    Book.create_table()
    yield Book, test_model
    Book.drop_table()
