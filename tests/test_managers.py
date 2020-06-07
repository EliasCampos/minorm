import pytest

from minorm.managers import QueryExpression, OrderByExpression


class TestQueryExpression:

    def test_filter(self, test_model):
        query = QueryExpression(model=test_model)

        result = query.filter(name='x', age__gt=2)

        assert result is query
        assert str(result._where) == "name = ? AND age > ?"
        assert result._where.values() == ('x', 2)

    def test_aswell(self, test_model):
        query = QueryExpression(model=test_model)

        result = query.filter(age__in=(3, 4)).aswell(age__lte=5)

        assert result is query
        assert str(result._where) == "age IN ? OR age <= ?"
        assert result._where.values() == ((3, 4), 5)

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
