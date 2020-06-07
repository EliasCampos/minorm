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

    def test_to_sql(self, mocker, fake_db):
        mocker.patch('minorm.models.get_default_db')

        class Person(Model):
            name = CharField(max_length=50)
            age = IntegerField(null=True, default=42)

            class Meta:
                db = fake_db

        assert Person.to_sql() == ("CREATE TABLE person ("
                                   "name VARCHAR(50) NOT NULL, "
                                   "age INTEGER DEFAULT 42, "
                                   "id INTEGER PRIMARY KEY AUTOINCREMENT);")

    def test_create_table(self, test_db):
        class Person(Model):
            name = CharField(max_length=50)
            age = IntegerField(null=True, default=42)

            class Meta:
                db = test_db

        Person.create_table()

        select_tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
        result = test_db.execute(select_tables_query, fetch=True)
        assert (Person._meta.table_name, ) in result

    def test_drop_table(self, test_db):
        select_tables_query = "CREATE TABLE test_table (id INTEGER PRIMARY KEY AUTOINCREMENT, x INTEGER)"
        test_db.execute(select_tables_query)

        class Person(Model):
            x = IntegerField(null=True)

            class Meta:
                table_name = "test_table"
                db = test_db

        Person.drop_table()

        select_tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
        result = test_db.execute(select_tables_query, fetch=True)
        assert (Person._meta.table_name, ) not in result

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
        assert person.pk is None

    def test_save(self, test_model):
        instance = test_model(name="john", age=33)
        instance.save()

        assert instance.pk == 1

        instance2 = test_model(name="steven", age=19)
        instance2.save()

        assert instance2.pk == 2

        instance.age = "42"
        instance.save()

        assert instance.pk == 1
        assert instance.age == 42
