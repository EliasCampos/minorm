import pytest

from minorm.exceptions import MultipleQueryResult
from minorm.managers import QueryExpression, OrderByExpression


class TestQueryExpression:

    def test_filter(self, test_model):
        query = QueryExpression(model=test_model)

        result = query.filter(name='x', age__gt=2)

        assert result is query
        assert str(result._where) == "person.name = {0} AND person.age > {0}"
        assert result._where.values() == ('x', 2)

    def test_aswell(self, test_model):
        query = QueryExpression(model=test_model)

        result = query.filter(age__in=(3, 4)).aswell(age__lte=5)

        assert result is query
        assert str(result._where) == "person.age IN ({0}, {0}) OR person.age <= {0}"
        assert result._where.values() == (3, 4, 5)

    def test_order_by(self, test_model):
        query = QueryExpression(model=test_model)

        result = query.order_by('-age', 'name')

        assert result is query
        assert result._order_by == {OrderByExpression('age', 'DESC'), OrderByExpression('name', 'ASC')}

    def test_update(self, test_model):
        db = test_model.db

        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('John', 19))
        test_model.query.update(age=42)

        results = db.execute('SELECT name, age FROM person;', fetch=True)
        assert results[0][1] == 42

    def test_update_filter(self, test_model):
        db = test_model.db

        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', many=True, params=[('x', 3), ('y', 5), ('z', 1)])

        test_model.query.filter(age__lt=5).update(name="test")

        assert db.last_query_rowcount == 2

        results = db.execute('SELECT * FROM person WHERE name = ?;', ('test', ), fetch=True)
        assert len(results) == 2

    def test_create(self, test_model):
        test_model.query.create(name='Vasya', age=19)
        results = test_model.db.execute('SELECT * FROM person WHERE id = ?;', (1,), fetch=True)
        assert results

    def test_update_with_fk(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model.db

        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', params=('x', 3))
        db.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', params=('y', 1))

        author = external_model(name='x', age=3)
        setattr(author, 'id', 1)

        model_with_fk.query.filter(author=author).update(title='test')

        results = db.execute('SELECT * FROM book WHERE title = ?;', ('test', ), fetch=True)
        assert results

    def test_get(self, test_model):
        db = test_model.db

        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', params=('x', 3))

        instance = test_model.query.get(id=1)
        assert instance.pk == 1
        assert instance.name == 'x'
        assert instance.age == 3

    def test_get_does_not_exists(self, test_model):
        db = test_model.db
        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', params=('x', 3))

        with pytest.raises(test_model.DoesNotExists):
            test_model.query.get(id=9000)

    def test_get_multiple_result(self, test_model):
        db = test_model.db
        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', many=True, params=[('x', 10), ('y', 10)])

        with pytest.raises(MultipleQueryResult):
            test_model.query.get(age=10)

    def test_first(self, test_model):
        db = test_model.db
        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', many=True, params=[('x', 3), ('y', 6), ('z', 6)])

        instance = test_model.query.first()
        assert instance.pk == 1
        assert instance.name == 'x'

        instance = test_model.query.filter(age=6).first()
        assert instance.pk == 2
        assert instance.name == 'y'

        instance = test_model.query.filter(age=9000).first()
        assert not instance

    def test_all(self, test_model):
        db = test_model.db
        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', many=True, params=[('x', 3), ('y', 6), ('z', 6)])

        results = test_model.query.all()
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

        result = test_model.query.bulk_create([instance1, instance2, 'foobar'])
        assert result == 2

        db = test_model.db

        result1 = db.execute('SELECT * FROM person WHERE name = ? AND age = ?;', params=('John', 33), fetch=True)
        assert result1

        result2 = db.execute('SELECT * FROM person WHERE name = ? AND age = ?;', params=('Dick', 42), fetch=True)
        assert result2

    def test_select_related(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model.db

        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', params=('x', 3))
        db.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', params=('y', 1))

        query = model_with_fk.query
        related_query = query.select_related('author')

        assert related_query is query

        result = related_query.get(id=1)

        assert result.pk == 1
        assert result.author.pk == 1
        assert result.author.name == 'x'
        assert result.author.age == 3

    def test_select_related_namedtuple(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model.db

        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', many=True, params=[('foo', 18), ('bar', 19)])
        db.execute('INSERT INTO book (title, person_id) VALUES (?, ?);',
                   many=True, params=[('a', 1), ('b', 2), ('c', 1)])

        results = model_with_fk.query.select_related('author').all()

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
        db = test_model.db
        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', many=True, params=[('x', 3), ('y', 6), ('z', 6)])

        results = test_model.query[:2].all()

        assert len(results) == 2

        assert results[0].id == 1
        assert results[1].id == 2

    def test_index(self, test_model):
        db = test_model.db
        db.execute('INSERT INTO person (name, age) VALUES (?, ?);', many=True, params=[('x', 3), ('y', 6), ('z', 6)])

        result = test_model.query.filter(id__in=[2, 3])[1]

        assert result.id == 3
        assert result.name == 'z'
