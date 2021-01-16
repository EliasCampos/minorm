import pytest

from minorm.fields import AutoField, CharField, IntegerField
from minorm.managers import QuerySet
from minorm.models import Model, ModelSetupError


class TestModel:

    def test_new(self, test_db):
        class Person(Model):
            name = CharField(max_length=120)
            age = IntegerField(column_name='test_column')

            class Meta:
                table_name = 'test_model_table'
                db = test_db

        assert len(Person._meta.fields) == 2 + 1  # custom fields + auto created primary key
        assert Person._meta.fields[0].column_name == 'name'
        assert Person._meta.fields[1].column_name == 'test_column'
        assert Person._meta.fields[2].column_name == 'id'

        assert Person._meta.db == test_db
        assert Person._meta.table_name == 'test_model_table'

    def test_new_multiple_pks(self, test_db):
        with pytest.raises(ModelSetupError, match=r'.*primary\s+key.*'):
            class SomeModel(Model):
                id = AutoField(pk=True)
                name = CharField(max_length=120, pk=True)

                class Meta:
                    table_name = 'test_model_table'
                    db = test_db

    def test_qs(self):
        class Person(Model):
            name = CharField(max_length=255)
            age = IntegerField()

        qs = Person.qs
        assert isinstance(qs, QuerySet)
        assert qs.model == Person

    def test_render_sql(self, test_db):
        class Person(Model):
            name = CharField(max_length=50)
            last_name = CharField(max_length=44, null=True)
            age = IntegerField(null=True, default=42)
            score = IntegerField(null=True, default=None)

            class Meta:
                db = test_db

        assert Person.render_sql() == (
            "CREATE TABLE person ("
            "name VARCHAR(50) NOT NULL, "
            "last_name VARCHAR(44), "
            "age INTEGER, "
            "score INTEGER, "
            "id INTEGER PRIMARY KEY AUTOINCREMENT);"
        )

    def test_create_table(self, test_db):
        class Person(Model):
            name = CharField(max_length=50)
            age = IntegerField(null=True, default=42)

            class Meta:
                db = test_db

        Person.create_table()

        select_tables_query = "SELECT name FROM sqlite_master WHERE type='table'"

        with test_db.cursor() as curr:
            curr.execute(select_tables_query)
            result = curr.fetchall()
        assert tuple(result[0]) == (Person._meta.table_name, )

    def test_drop_table(self, test_db):
        create_tables_query = "CREATE TABLE test_table (id INTEGER PRIMARY KEY AUTOINCREMENT, x INTEGER)"
        with test_db.cursor() as curr:
            curr.execute(create_tables_query)

        class Person(Model):
            x = IntegerField(null=True)

            class Meta:
                table_name = "test_table"
                db = test_db

        Person.drop_table()

        select_tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
        with test_db.cursor() as curr:
            curr.execute(select_tables_query)
            result = curr.fetchall()
        assert (Person._meta.table_name, ) not in result

    def test_check_field(self, test_db):
        class Person(Model):
            title = CharField(max_length=120)

        result = Person._meta.check_field('title')
        assert result.name == 'title'

    def test_check_field_invalid_field(self, test_db):
        class Person(Model):
            name = CharField(max_length=120)

        with pytest.raises(ValueError, match='.*age.*'):
            Person._meta.check_field('age')

    def test_init(self, test_db):
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

    def test_save_update(self, test_model):
        prev_instance = test_model(name="john", age=33)
        prev_instance.save()

        new_instance = test_model(name="steven", age=19)
        new_instance.save()

        new_instance.age = 20
        new_instance.save()

        select_previous_query = "SELECT name, age FROM person ORDER BY id"
        with test_model._meta.db.cursor() as curr:
            curr.execute(select_previous_query)
            result = curr.fetchall()

        old_row = result[0]
        assert old_row[0] == 'john'
        assert old_row[1] == 33

        new_row = result[1]
        assert new_row[0] == 'steven'
        assert new_row[1] == 20

    def test_save_with_fk(self, related_models):
        model_with_fk, external_model = related_models

        author = external_model(name='Steven', age=19)
        author.save()

        book = model_with_fk(author=author)
        book.title = "The Dark Tower"
        book.save()

        assert book.pk == 1
        assert book.author_id == author.pk
        assert book._author_cached is author

    def test_refresh_from_db(self, test_model):
        instance = test_model(name="john", age=33)
        instance.save()
        test_model.qs.filter(id=instance.id).update(name='foobar', age=42)
        instance.refresh_from_db()

        assert instance.pk == 1
        assert instance.name == 'foobar'
        assert instance.age == 42

    def test_delete(self, test_model):
        instance = test_model(name="john", age=33)
        instance.save()
        instance_id = instance.pk

        result = instance.delete()
        assert result == 1
        assert not instance.pk
        assert not test_model.qs.filter(id=instance_id).exists()
