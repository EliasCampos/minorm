import pytest

from minorm.connectors import connector
from minorm.db_specs import SQLiteSpec
from minorm.fields import CharField, IntegerField, ForeignKey
from minorm.models import Model


@pytest.fixture(scope="function")
def test_db():
    db = connector.connect(SQLiteSpec(":memory:"))
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
        author = ForeignKey(to=test_model)

        class Meta:
            db = test_model._meta.db

    Book.create_table()
    yield Book, test_model
    Book.drop_table()
