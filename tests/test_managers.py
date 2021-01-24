import pytest

from minorm.exceptions import MultipleQueryResult
from minorm.fields import CharField, ForeignKey
from minorm.managers import QuerySet, OrderByExpression
from minorm.models import Model


class TestQuerySet:

    def test_filter_query(self, test_model):
        qs = QuerySet(model=test_model)

        result = qs.filter(name='x', age__gt=2)

        assert str(result._where) == "person.name = {0} AND person.age > {0}"
        assert result._where.values() == ('x', 2)

    def test_filter_contains_query(self, test_model):
        qs = QuerySet(model=test_model)

        result = qs.filter(name__contains='foo')
        assert str(result._where) == "person.name LIKE {0}"
        assert result._where.values() == ('%foo%',)

    def test_filter_startswith_query(self, test_model):
        qs = QuerySet(model=test_model)

        result = qs.filter(name__startswith='bar')
        assert str(result._where) == "person.name LIKE {0}"
        assert result._where.values() == ('bar%',)

    def test_filter_endswith_query(self, test_model):
        qs = QuerySet(model=test_model)

        result = qs.filter(name__endswith='baz')
        assert str(result._where) == "person.name LIKE {0}"
        assert result._where.values() == ('%baz',)

    def test_aswell(self, test_model):
        qs = QuerySet(model=test_model)

        result = qs.filter(age__in=(3, 4)).aswell(age__lte=5)

        assert str(result._where) == "person.age IN ({0}, {0}) OR person.age <= {0}"
        assert result._where.values() == (3, 4, 5)

    def test_order_by(self, test_model):
        qs = QuerySet(model=test_model)

        result = qs.order_by('-age', 'name')

        assert result._order_by == [OrderByExpression('person.age', 'DESC'), OrderByExpression('person.name', 'ASC')]

    def test_filter_by_pk(self, test_model):
        qs1 = test_model.qs.filter(pk=1)
        assert str(qs1._where) == 'person.id = {0}'
        assert qs1._where.values() == (1,)

        qs2 = test_model.qs.filter(pk__in=(1, 2))
        assert str(qs2._where) == 'person.id IN ({0}, {0})'
        assert qs2._where.values() == (1, 2)

    def test_filter_by_pk_db_hit(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('A', 11))
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('B', 12))

        qs1 = test_model.qs.filter(pk=1)
        assert len(qs1.fetch()) == 1

        qs2 = test_model.qs.filter(pk__in=(1, 2))
        assert len(qs2.fetch()) == 2

        qs3 = test_model.qs.filter(pk=42)
        assert not qs3.fetch()

    def test_filter_fk_fields(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('A', 17))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('a', 1))
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('B', 18))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('b', 2))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('d', 2))
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('C', 19))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('c', 3))

        qs = model_with_fk.qs.filter(author__name__in=('A', 'B'), author__age__gte=18)
        results = qs.fetch()
        assert len(results) == 2
        assert results[0].title == 'b'
        assert results[0].author == 2
        assert results[1].title == 'd'
        assert results[1].author == 2

    def test_aswell_fk_fields(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('A', 17))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('a', 1))
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('B', 18))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('b', 2))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('d', 2))
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('C', 19))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('c', 3))

        qs = model_with_fk.qs.filter(author__name='A').aswell(author__age__gt=18)
        results = qs.fetch()
        assert len(results) == 2
        assert results[0].title == 'a'
        assert results[0].author == 1
        assert results[1].title == 'c'
        assert results[1].author == 3

    def test_filter_own_and_fk_fields(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('A', 17))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('a', 1))
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('B', 18))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('a', 2))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('b', 2))

        qs = model_with_fk.qs.filter(title='a').filter(author__name='A')
        results = qs.fetch()
        assert len(results) == 1
        assert results[0].title == 'a'
        assert results[0].author == 1

    def test_filter_inner_relations(self, test_db):

        class L1(Model):
            title = CharField(max_length=100)

        class L2(Model):
            title = CharField(max_length=100)
            l11 = ForeignKey(L1)

        class L3(Model):
            title = CharField(max_length=100)
            l21 = ForeignKey(L2)

        L1.create_table()
        L2.create_table()
        L3.create_table()

        with test_db.cursor() as c:
            c.execute('INSERT INTO L1 (title) VALUES (?);', ('l11',))
            c.execute('INSERT INTO L1 (title) VALUES (?);', ('l12',))
            c.execute('INSERT INTO L1 (title) VALUES (?);', ('l111',))
            c.execute('INSERT INTO L2 (title, l1_id) VALUES (?, ?);', ('l21', 1))  # with l11
            c.execute('INSERT INTO L2 (title, l1_id) VALUES (?, ?);', ('l22', 2))
            c.execute('INSERT INTO L2 (title, l1_id) VALUES (?, ?);', ('l211', 1))  # also with l11
            c.execute('INSERT INTO L2 (title, l1_id) VALUES (?, ?);', ('l21', 3))
            c.execute('INSERT INTO L3 (title, l2_id) VALUES (?, ?);', ('l31', 1))
            c.execute('INSERT INTO L3 (title, l2_id) VALUES (?, ?);', ('l32', 2))
            c.execute('INSERT INTO L3 (title, l2_id) VALUES (?, ?);', ('l311', 3))
            c.execute('INSERT INTO L3 (title, l2_id) VALUES (?, ?);', ('l31x', 4))  # also with L2 = l21 but L1 != l11

        qs = L3.qs.filter(l21__l11__title='l11').select_related('l21')
        result = qs.fetch()
        assert len(result) == 2

        assert result[0].title == 'l31'
        assert result[0].l21.title == 'l21'
        assert result[0].l21.l11 == 1

        assert result[1].title == 'l311'
        assert result[1].l21.title == 'l211'
        assert result[1].l21.l11 == 1

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

    def test_get_by_pk(self, test_model):
        db = test_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('x', 3))

        instance = test_model.qs.get(pk=1)
        assert instance.pk == 1
        assert instance.name == 'x'

    def test_get_values(self, test_model):
        db = test_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('x', 3))

        instance = test_model.qs.values('name', 'age').get(id=1)
        assert instance == {'name': 'x', 'age': 3}

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

    def test_fetch(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 6), ('z', 6)])

        results = test_model.qs.fetch()
        assert results[0].id == 1
        assert results[0].name == 'x'
        assert results[0].age == 3

        assert results[1].id == 2
        assert results[1].name == 'y'
        assert results[1].age == 6

        assert results[2].id == 3
        assert results[2].name == 'z'
        assert results[2].age == 6

    def test_iter(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 20), ('y', 16), ('z', 19)])

        qs = test_model.qs.filter(age__gte=18)
        qs_iter = iter(qs)
        result = next(qs_iter)
        assert result.id == 1
        assert result.name == 'x'
        assert result.age == 20
        assert isinstance(result, test_model)

        result = next(qs_iter)
        assert result.id == 3
        assert result.name == 'z'
        assert result.age == 19

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

        result = related_qs.get(id=1)

        assert result.pk == 1
        assert result.author.pk == 1
        assert result.author.name == 'x'
        assert result.author.age == 3

    def test_select_related_multiple(self, test_db):

        class L11(Model):
            title = CharField(max_length=100)

        class L21(Model):
            title = CharField(max_length=100)
            l11 = ForeignKey(L11)

        class L22(Model):
            title = CharField(max_length=100)
            l11 = ForeignKey(L11, null=True)

        class L31(Model):
            title = CharField(max_length=100)
            l21 = ForeignKey(L21)
            l22 = ForeignKey(L22)
            l21_other = ForeignKey(L21, column_name='l21_other')

        L11.create_table()
        L21.create_table()
        L22.create_table()
        L31.create_table()

        with test_db.cursor() as c:
            c.execute('INSERT INTO L11 (title) VALUES (?);', ('l11',))
            c.execute('INSERT INTO L21 (title, l11_id) VALUES (?, ?);', ('l20', 1))
            c.execute('INSERT INTO L22 (title) VALUES (?);', ('l2x',))
            c.execute('INSERT INTO L22 (title) VALUES (?);', ('l2y',))
            c.execute('INSERT INTO L22 (title, l11_id) VALUES (?, ?);', ('l22', 1))
            c.execute('INSERT INTO L21 (title, l11_id) VALUES (?, ?);', ('l21', 1))
            c.execute('INSERT INTO L31 (title, l21_id, l21_other, l22_id) VALUES (?, ?, ?, ?);', ('l31', 2, 1, 3))

        qs = L31.qs.select_related('l21__l11', 'l22')
        instance = qs.first()

        assert instance.pk == 1
        assert instance.title == 'l31'
        assert instance.l21.pk == 2
        assert instance.l21.title == 'l21'
        assert instance.l21.l11.pk == 1
        assert instance.l21.l11.title == 'l11'

        assert instance.l21_other_id == 1
        assert instance.l22.pk == 3
        assert instance.l22.title == 'l22'
        assert instance.l22.l11_id == 1

    def test_values_multiple_relations(self, test_db):

        class L11(Model):
            title = CharField(max_length=100)
            text = CharField(max_length=200)

        class L21(Model):
            title = CharField(max_length=100)
            l11 = ForeignKey(L11)

        class L22(Model):
            title = CharField(max_length=100)

        class L31(Model):
            title = CharField(max_length=100)
            l21 = ForeignKey(L21)
            l22 = ForeignKey(L22)

        L11.create_table()
        L21.create_table()
        L22.create_table()
        L31.create_table()

        with test_db.cursor() as c:
            c.execute('INSERT INTO L11 (title, text) VALUES (?, ?);', ('l11', "FOOBAR"))
            c.execute('INSERT INTO L21 (title, l11_id) VALUES (?, ?);', ('l20', 1))
            c.execute('INSERT INTO L22 (title) VALUES (?);', ('l2x',))
            c.execute('INSERT INTO L22 (title) VALUES (?);', ('l2y',))
            c.execute('INSERT INTO L22 (title) VALUES (?);', ('l22',))
            c.execute('INSERT INTO L21 (title, l11_id) VALUES (?, ?);', ('l21', 1))
            c.execute('INSERT INTO L31 (title, l21_id, l22_id) VALUES (?, ?, ?);', ('l31', 2, 3))

        qs = L31.qs.values('l21__l11__text', 'title', 'l22__title', 'l22')
        instance = qs.first()
        assert instance['l21__l11__text'] == "FOOBAR"
        assert instance['title'] == 'l31'
        assert instance['l22__title'] == 'l22'
        assert instance['l22'] == 3

    def test_select_related_namedtuple(self, related_models):
        model_with_fk, external_model = related_models

        db = external_model._meta.db

        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('foo', 18), ('bar', 19)])
            c.executemany('INSERT INTO book (title, person_id) VALUES (?, ?);', [('a', 1), ('b', 2), ('c', 1)])

        results = model_with_fk.qs.select_related('author').fetch()

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

        results = test_model.qs[:2].fetch()

        assert len(results) == 2

        assert results[0].id == 1
        assert results[1].id == 2

    def test_index(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 6), ('z', 6)])

        result = test_model.qs.filter(id__in=[1, 2])[1]
        assert result.id == 2
        assert result.name == 'y'

        last_result = test_model.qs[-1]
        assert last_result.id == 3

    def test_index_out_of_range(self, test_model):
        with pytest.raises(IndexError, match=r'^QuerySet\s+index\s+out\s+of\s+range$'):
            test_model.qs[9000]

    def test_values(self, test_model):
        db = test_model._meta.db
        with db.cursor() as c:
            c.executemany('INSERT INTO person (name, age) VALUES (?, ?);', [('x', 3), ('y', 6), ('z', 6)])

        result = test_model.qs.values('id', 'name').fetch()

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

        result = model_with_fk.qs.select_related('author').values('title', 'author__name').fetch()
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

    def test_exists_qs_attributes(self, test_model):
        qs = test_model.qs
        qs.exists()
        assert not qs._values_mapping  # should not change attributes of the queryset
        assert not qs._limit
