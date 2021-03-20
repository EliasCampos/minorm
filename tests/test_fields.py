import pytest
from decimal import Decimal
from datetime import date, datetime

from minorm.fields import (
    Field,
    AutoField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    FloatField,
    ForeignKey,
    IntegerField,
)


class TestField:

    def test_column_name(self):
        class Foo:
            bar = Field()

        assert Foo.bar.column_name == 'bar'

    def test_render_sql(self, mocker):
        mocker.patch.object(Field, 'render_sql_type', lambda obj: 'INTEGER')

        unique_field = Field(null=False, unique=True, default=42, column_name='test')
        assert unique_field.render_sql() == 'test INTEGER NOT NULL UNIQUE'

        non_unique = Field(null=True, unique=False, column_name='test', default=None)
        assert non_unique.render_sql() == 'test INTEGER'

    @pytest.mark.parametrize('lookup_name, operator', [
        ('lt', '<'),
        ('lte', '<='),
        ('gt', '>'),
        ('gte', '>='),
        ('neq', '!='),
    ])
    def test_resolve_lookup(self, lookup_name, operator):
        field = Field(column_name='test_column')

        result = field.resolve_lookup(lookup_name, 'test_value', 'test_table')
        assert result.field == 'test_table.test_column'
        assert result.op == operator
        assert result.value == 'test_value'

    def test_resolve_lookup_in(self):
        field = Field(column_name='test_column')

        result = field.resolve_lookup('in', ('foo', 'bar'), 'test_table')
        assert result.field == 'test_table.test_column'
        assert result.op == 'IN'
        assert result.value == ['foo', 'bar']

    def test_resolve_wrong_lookup(self):
        field = Field(column_name='test_column')

        result = field.resolve_lookup('NOWAY', 'test_value', 'test_table')
        assert result is None


class TestIntegerField:

    def test_render_sql_type(self):
        int_field = IntegerField()
        assert int_field.render_sql_type() == 'INTEGER'

    def test_to_query_parameter(self):
        int_field = IntegerField()
        assert int_field.to_query_parameter(None) is None
        assert int_field.to_query_parameter(9) == 9
        assert int_field.to_query_parameter("12") == 12

    @pytest.mark.parametrize('invalid_value, expected_error_type', [
        pytest.param("foo-bar", ValueError, id='non-number-string'),
        pytest.param((), TypeError, id="incorrect-object-type"),
    ])
    def test_to_query_parameter_invalid_value(self, invalid_value, expected_error_type):
        int_field = IntegerField()
        int_field._name = 'test_field'

        with pytest.raises(expected_error_type, match=r'"test_field"\s+expected\s+a\s+number'):
            int_field.to_query_parameter(invalid_value)


class TestFloatField:

    def test_render_sql_type(self):
        float_field = FloatField()
        assert float_field.render_sql_type() == 'REAL'

    def test_to_query_parameter(self):
        float_field = FloatField()
        assert float_field.to_query_parameter(None) is None
        assert float_field.to_query_parameter(3.14) == 3.14
        assert float_field.to_query_parameter("2.718") == 2.718
        assert float_field.to_query_parameter(42) == 42


class TestBooleanField:

    def test_render_sql_type(self):
        bool_field = BooleanField()
        assert bool_field.render_sql_type() == 'BOOLEAN'

    @pytest.mark.parametrize('correct_value, converted_value', [
        pytest.param(None, None, id='null-is-correct'),
        pytest.param(True, True, id='true-is-correct'),
        pytest.param(True, True, id='true-is-correct'),
        pytest.param(False, False, id='false-is-correct'),
        pytest.param(1, True, id='true-and-1-are-equal'),
        pytest.param(0, False, id='false-and-0-are-equal'),
    ])
    def test_to_query_parameter(self, correct_value, converted_value):
        bool_field = BooleanField()
        assert bool_field.to_query_parameter(correct_value) is converted_value

    @pytest.mark.parametrize('invalid_value', [
        pytest.param(13, id='number-not-valid-value'),
        pytest.param("nope", id="string-not-valid-value"),
        pytest.param((), id="tuple-not-valid-value")
    ])
    def test_to_query_parameter_invalid_value(self, invalid_value):
        bool_field = BooleanField()
        bool_field._name = 'test_field'

        with pytest.raises(ValueError, match=r'"test_field"\s+value\s+must\s+be\s+either\s+True\s+or\s+False'):
            bool_field.to_query_parameter(invalid_value)


class TestCharField:

    def test_render_sql_type(self):
        char_field = CharField(max_length=100)
        assert char_field.render_sql_type() == 'VARCHAR(100)'

    @pytest.mark.parametrize('value, expected', [
        pytest.param("Foo bar", 'Foo bar', id='string-value'),
        pytest.param(b'test', 'test', id='bytes-value'),
        pytest.param(42, '42', id='int-value'),
        pytest.param(None, None, id='null-value'),
    ])
    def test_to_query_parameter(self, value, expected):
        char_field = CharField(max_length=255)
        assert char_field.to_query_parameter(value) == expected


class TestDecimalField:

    def test_render_sql_type(self):
        decimal_field = DecimalField(max_digits=4, decimal_places=2)
        assert decimal_field.render_sql_type() == 'DECIMAL(4, 2)'

    @pytest.mark.parametrize('value, expected', [
        pytest.param(4.445, Decimal("4.445"), id='float-value'),
        pytest.param(-1, Decimal("-1"), id='int-value'),
        pytest.param("3.14", Decimal("3.14"), id='string-valid-decimal'),
        pytest.param(None, None, id='null-value'),
    ])
    def test_to_query_parameter(self, value, expected):
        decimal_field = DecimalField(max_digits=6, decimal_places=2)
        assert decimal_field.to_query_parameter(value) == expected

    @pytest.mark.parametrize('invalid_value', [
        pytest.param("foobar", id='sting-non-decimal-not-valid-value'),
        pytest.param((), id="tuple-not-valid-value")
    ])
    def test_to_query_parameter_invalid_value(self, invalid_value):
        decimal_field = DecimalField(max_digits=6, decimal_places=2)
        decimal_field._name = 'test_field'

        with pytest.raises(ValueError, match=r'"test_field"\s+value\s+must\s+be\sa\s+decimal\s+number'):
            decimal_field.to_query_parameter(invalid_value)


class TestDateField:

    def test_render_sql_type(self):
        date_field = DateField()
        assert date_field.render_sql_type() == 'DATE'

    @pytest.mark.parametrize('value, expected', [
        pytest.param('2020-09-02', date(2020, 9, 2), id='string-value-as-date'),
        pytest.param(date(2020, 9, 2), date(2020, 9, 2), id='date-value'),
        pytest.param(datetime(2020, 9, 2, 11, 42), date(2020, 9, 2), id='date-time-value'),
        pytest.param(None, None, id='null-value'),
    ])
    def test_to_query_parameter(self, value, expected):
        date_field = DateField()
        assert date_field.to_query_parameter(value) == expected

    @pytest.mark.parametrize('invalid_value, expected_error_type', [
        pytest.param("foobar", ValueError, id='non-iso-datetime-string'),
        pytest.param('2020-09-02 11:42', ValueError, id='with-time-not-allowed'),
        pytest.param((), TypeError, id="incorrect-object-type"),
    ])
    def test_to_query_parameter_invalid_value(self, invalid_value, expected_error_type):
        date_field = DateField()
        date_field._name = 'test_field'

        with pytest.raises(expected_error_type):
            date_field.to_query_parameter(invalid_value)


class TestDateTimeField:

    def test_render_sql_type(self):
        date_field = DateTimeField()
        assert date_field.render_sql_type() == 'TIMESTAMP'

    @pytest.mark.parametrize('value, expected', [
        pytest.param('2020-09-02 11:42:33.777', datetime(2020, 9, 2, 11, 42, 33, 777000), id='string-value-with-ms'),
        pytest.param('2020-09-02 11:42:33', datetime(2020, 9, 2, 11, 42, 33), id='string-value-with-seconds'),
        pytest.param('2020-09-02 11:42', datetime(2020, 9, 2, 11, 42), id='string-value-with-minutes'),
        pytest.param('2020-09-02', datetime(2020, 9, 2), id='string-value-as-date'),
        pytest.param(date(2020, 9, 2), datetime(2020, 9, 2), id='date-value'),
        pytest.param(datetime(2020, 9, 2, 11, 42), datetime(2020, 9, 2, 11, 42), id='date-time-value'),
        pytest.param(None, None, id='null-value'),
    ])
    def test_to_query_parameter(self, value, expected):
        date_field = DateTimeField()
        assert date_field.to_query_parameter(value) == expected

    @pytest.mark.parametrize('invalid_value, expected_error_type', [
        pytest.param("foobar", ValueError, id='non-iso-datetime-string'),
        pytest.param((), TypeError, id="incorrect-object-type"),
    ])
    def test_to_query_parameter_invalid_value(self, invalid_value, expected_error_type):
        date_field = DateTimeField()
        date_field._name = 'test_field'

        with pytest.raises(expected_error_type):
            date_field.to_query_parameter(invalid_value)


class TestAutoField:

    def test_render_sql_type(self, test_model):
        auto_field = AutoField()
        auto_field._model = test_model
        assert auto_field.render_sql_type() == 'INTEGER'  # for sqlite db in model fixture

    def test_render_sql(self, test_model):
        auto_field = AutoField(column_name='test_field')
        auto_field._model = test_model
        assert auto_field.render_sql() == 'test_field INTEGER NOT NULL AUTOINCREMENT'  # for sqlite db in model fixture

    def test_render_sql_is_pk(self, test_model):
        auto_field = AutoField(column_name='test_field', pk=True)
        auto_field._model = test_model
        assert auto_field.render_sql() == 'test_field INTEGER PRIMARY KEY AUTOINCREMENT'  # for sqlite db in fixture


class TestForeignKey:

    def test_render_sql_type(self, test_model):
        fk = ForeignKey(to=test_model, null=False)
        assert fk.render_sql_type() == "INTEGER REFERENCES person (id)"  # table name and pk are from model fixture

    def test_column_name(self, test_model):
        fk = ForeignKey(to=test_model, null=False)
        assert fk.column_name == "person_id"  # table name is from model fixture

    def test_to_query_parameter(self, related_models):
        model_with_fk, external_model = related_models
        author = external_model(id=1)

        field = model_with_fk._meta.get_fk_field('author')
        assert field.to_query_parameter(author) == 1

    def test_set_related_obj(self, related_models):
        model_with_fk, external_model = related_models

        author = external_model(id=1)
        book = model_with_fk(id=1)
        book.author = author
        assert book.author_id == 1
        assert book._author_cached is author

    def test_set_raw_fk(self, related_models):
        model_with_fk, external_model = related_models

        book = model_with_fk(id=1)
        assert book.author_id is None
        book.author = 1
        assert book.author_id == 1

    def test_get(self, related_models):
        model_with_fk, external_model = related_models
        db = external_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('foo', 10))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('a', 1))

        instance = model_with_fk(id=1, author_id=1)
        author = instance.author  # should perform db query
        assert author.id == 1
        assert author.name == 'foo'

    def test_get_no_fk_value(self, related_models):
        model_with_fk, external_model = related_models
        db = external_model._meta.db

        with db.cursor() as c:
            c.execute('INSERT INTO person (name, age) VALUES (?, ?);', ('foo', 10))
            c.execute('INSERT INTO book (title, person_id) VALUES (?, ?);', ('a', 1))

        instance = model_with_fk(id=1)  # fk value not passed
        author = instance.author
        assert author is None
