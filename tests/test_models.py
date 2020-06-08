import pytest

from minorm.db import SQLiteDatabase
from minorm.fields import CharField, IntegerField, ForeignKey
from minorm.managers import QuerySet
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

    def test_objects(self, mocker, fake_db):
        db_mock = mocker.patch('minorm.models.get_default_db')
        db_mock.return_value = fake_db

        class Person(Model):
            name = CharField(max_length=255)
            age = IntegerField()

        qs = Person.objects
        assert isinstance(qs, QuerySet)
        assert qs.model == Person

    def test_to_sql(self, mocker, fake_db):
        db_mock = mocker.patch('minorm.models.get_default_db')
        db_mock.return_value = fake_db

        class Person(Model):
            name = CharField(max_length=50)
            last_name = CharField(max_length=44, null=True)
            age = IntegerField(null=True, default=42)
            score = IntegerField(null=True, default=None)

            class Meta:
                db = fake_db

        assert Person.to_sql() == ("CREATE TABLE person ("
                                   "name VARCHAR(50) NOT NULL, "
                                   "last_name VARCHAR(44), "
                                   "age INTEGER DEFAULT 42, "
                                   "score INTEGER DEFAULT NULL, "
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
        assert tuple(result[0]) == (Person._meta.table_name, )

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

    def test_check_field(self, mocker, fake_db):
        db_mock = mocker.patch('minorm.models.get_default_db')
        db_mock.return_value = fake_db

        class Person(Model):
            name = CharField(max_length=120)

        with pytest.raises(ValueError, match='.*age.*'):
            Person.check_field('age')

    def test_init(self, mocker, fake_db):
        db_mock = mocker.patch('minorm.models.get_default_db')
        db_mock.return_value = fake_db

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

    def test_save_with_fk(self, related_models):
        model_with_fk, external_model = related_models

        author = external_model(name='Steven', age=19)
        author.save()

        book = model_with_fk(author=author)
        book.title = "The Dark Tower"
        book.save()

        assert book.pk == 1
        assert book.author == author.pk

    def test_refresh_from_db(self, test_model):
        instance = test_model(name="john", age=33)
        instance.save()

        test_model.objects.filter(id=instance.id).update(name='foobar', age=42)

        instance.refresh_from_db()

        assert instance.pk == 1
        assert instance.name == 'foobar'
        assert instance.age == 42
