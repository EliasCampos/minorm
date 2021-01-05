import pytest

from minorm.exceptions import MultipleQueryResult
from minorm.managers import QuerySet, OrderByExpression


class TestQueryExpression:

    def test_filter_query(self, test_model):
        query = QuerySet(model=test_model)

        result = query.filter(name='x', age__gt=2)

        assert result is query
        assert str(result._where) == "person.name = {0} AND person.age > {0}"
        assert result._where.values() == ('x', 2)

    def test_filter_contains_query(self, test_model):
        query = QuerySet(model=test_model)

        result = query.filter(name__contains='foo')
        assert str(result._where) == "person.name LIKE {0}"
        assert result._where.values() == ('%foo%',)

    def test_filter_startswith_query(self, test_model):
        query = QuerySet(model=test_model)

        result = query.filter(name__startswith='bar')
        assert str(result._where) == "person.name LIKE {0}"
        assert result._where.values() == ('bar%',)

    def test_filter_endswith_query(self, test_model):
        query = QuerySet(model=test_model)

        result = query.filter(name__endswith='baz')
        assert str(result._where) == "person.name LIKE {0}"
        assert result._where.values() == ('%baz',)

    def test_aswell(self, test_model):
        query = QuerySet(model=test_model)

        result = query.filter(age__in=(3, 4)).aswell(age__lte=5)

        assert result is query
        assert str(result._where) == "person.age IN ({0}, {0}) OR person.age <= {0}"
        assert result._where.values() == (3, 4, 5)

    def test_order_by(self, test_model):
        query = QuerySet(model=test_model)

        result = query.order_by('-age', 'name')

        assert result is query
        assert result._order_by == {OrderByExpression('person.age', 'DESC'), OrderByExpression('person.name', 'ASC')}

    def test_update(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('John', 19))

        test_model.qs.update(age=42)

        with db.cursor() as c:
            c.execute('SELECT name, age FROM person;')
            results = c.fetchall()
        assert results[0][1] == 42

    def test_update_filter(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 5), ('z', 1)])

        rowcount = test_model.qs.filter(age__lt=5).update(name="test")

        assert rowcount == 2
        with db.cursor() as c:
            c.execute('SELECT * FROM person WHERE name = ?;', ('test',))
            results = c.fetchall()
        assert len(results) == 2

    def test_create(self, test_model):
        test_model.qs.create(name='Vasya', age=19)
        with test_model._meta.db.cursor() as c:
            c.execute('SELECT * FROM person WHERE id = ?;', (1,))
            results = c.fetchall()
        assert results

    def test_update_with_fk(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('x', 3))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('y', 1))

        author = external_model(name='x', age=3)
        setattr(author, 'id', 1)

        model_with_fk.qs.filter(author=author).update(title='test')

        with db.cursor() as c:
            c.execute('SELECT * FROM book WHERE title = ?;', ('test', ))
            results = c.fetchall()
        assert results

    def test_delete(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 7), ('z', 6)])

        result = test_model.qs.filter(age__gt=3).delete()
        assert result == 2
        with db.cursor() as c:
            c.execute('SELECT * FROM person;')
            rows = c.fetchall()
        assert len(rows) == 1

    def test_get(self, test_model):
        db = test_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('x', 3))

        instance = test_model.qs.get(id=1)
        assert instance.pk == 1
        assert instance.name == 'x'
        assert instance.age == 3

    def test_get_does_not_exists(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('x', 3))

        with pytest.raises(test_model.DoesNotExists):
            test_model.qs.get(id=9000)

    def test_get_multiple_result(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 10), ('y', 10)])

        with pytest.raises(MultipleQueryResult):
            test_model.qs.get(age=10)

    def test_first(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 6), ('z', 6)])

        instance = test_model.qs.first()
        assert instance.pk == 1
        assert instance.name == 'x'

        instance = test_model.qs.filter(age=6).first()
        assert instance.pk == 2
        assert instance.name == 'y'

        instance = test_model.qs.filter(age=9000).first()
        assert not instance

    def test_all(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 6), ('z', 6)])

        results = test_model.qs.all()
        assert results[0].id == 1
        assert results[0].name == 'x'
        assert results[0].age == 3

        assert results[1].id == 2
        assert results[1].name == 'y'
        assert results[1].age == 6

        assert results[2].id == 3
        assert results[2].name == 'z'
        assert results[2].age == 6

    def test_bulk_create(self, test_model):
        instance1 = test_model(name='John', age=33)
        instance2 = test_model(name='Dick', age=42)

        result = test_model.qs.bulk_create([instance1, instance2, 'foobar'])
        assert result == 2

        db = test_model._meta.db

        with db.cursor() as c:
            c.execute('SELECT * FROM person WHERE name = ? AND age = ?;', ('John', 33))
            result1 = c.fetchall()
        assert result1

        with db.cursor() as c:
            c.execute('SELECT * FROM person WHERE name = ? AND age = ?;', ('Dick', 42))
            result2 = c.fetchall()
        assert result2

    def test_select_related(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model._meta.db
        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('x', 3))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('y', 1))

        qs = model_with_fk.qs
        related_qs = qs.select_related('author')

        assert related_qs is qs

        result = related_qs.get(id=1)

        assert result.pk == 1
        assert result.author.pk == 1
        assert result.author.name == 'x'
        assert result.author.age == 3

    def test_select_related_namedtuple(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model._meta.db

        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('foo', 18), ('bar', 19)])
            c.executemany('INSERT INTO book (title, person_id) VALUES (?, ?);', [('a', 1), ('b', 2), ('c', 1)])

        results = model_with_fk.qs.select_related('author').all()

        assert results[0].author.id == 1
        assert results[0].author.name == 'foo'
        assert results[0].author.age == 18

        assert results[1].author.id == 2
        assert results[1].author.name == 'bar'
        assert results[1].author.age == 19

        assert results[2].author.id == 1
        assert results[2].author.name == 'foo'
        assert results[2].author.age == 18

    def test_limit(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 6), ('z', 6)])

        results = test_model.qs[:2].all()

        assert len(results) == 2

        assert results[0].id == 1
        assert results[1].id == 2

    def test_index(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 6), ('z', 6)])

        result = test_model.qs.filter(id__in=[2, 3])[1]

        assert result.id == 3
        assert result.name == 'z'

    def test_values(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 6), ('z', 6)])

        result = test_model.qs.values('id', 'name').all()

        assert result[0]["id"] == 1
        assert result[0]["name"] == 'x'
        assert "age" not in result[0]

        assert result[1]["id"] == 2
        assert result[1]["name"] == 'y'
        assert "age" not in result[1]

        assert result[2]["id"] == 3
        assert result[2]["name"] == 'z'
        assert "age" not in result[2]

    def test_values_select_related(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('foo', 18), ('bar', 19)])
            c.executemany('INSERT INTO book (title, person_id) VALUES (?, ?);', [('a', 1), ('b', 2), ('c', 1)])

        result = model_with_fk.qs.select_related('author').values('title', 'author__name').all()
        assert result[0]["title"] == 'a'
        assert result[0]["author__name"] == 'foo'
        assert 'id' not in result[0]
        assert 'author_name' not in result[0]

        assert result[1]["title"] == 'b'
        assert result[1]["author__name"] == 'bar'

        assert result[2]["title"] == 'c'
        assert result[2]["author__name"] == 'foo'

    def test_exists(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('x', 3))

        assert test_model.qs.filter(name='x').exists()
        assert test_model.qs.filter(age=3).exists()

        assert not test_model.qs.filter(name='foobar').exists()
        assert not test_model.qs.filter(age=13).exists()
