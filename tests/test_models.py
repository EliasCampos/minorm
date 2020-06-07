import pytest

from minorm.db import SQLiteDatabase
from minorm.fields import CharField, IntegerField
from minorm.managers import QueryExpression
from minorm.models import Model


@pytest.fixture(scope="function")
def fake_db():
    return SQLiteDatabase("sqlite://")


class TestModel:

    def test_new(self, fake_db, mocker):
        mocker.patch('minorm.models.get_default_db')

        class Person(Model):
            name = CharField(max_length=120)
            age = IntegerField(column_name='test_column')

            class Meta:
                table_name = 'test_model_table'
                db = fake_db

        assert 'name' in Person._fields
        assert Person._fields['name'].column_name == 'name'

        assert 'age' in Person._fields
        assert Person._fields['age'].column_name == 'test_column'

        assert Person._meta.db == fake_db
        assert Person._meta.table_name == 'test_model_table'

    def test_query(self, mocker):
        mocker.patch('minorm.models.get_default_db')

        class Person(Model):
            name = CharField(max_length=255)
            age = IntegerField()

        query = Person.query
        assert isinstance(query, QueryExpression)
        assert query.model == Person

    def test_check_field(self, mocker):
        mocker.patch('minorm.models.get_default_db')

        class Person(Model):
            name = CharField(max_length=120)

        with pytest.raises(ValueError, match='.*age.*'):
            Person.check_field('age')

    def test_init(self, mocker):
        mocker.patch('minorm.models.get_default_db')

        class Person(Model):
            name = CharField(max_length=120)
            age = IntegerField(default=19)

        person = Person(name="john")
        assert person.name == "john"
        assert person.age == 19
