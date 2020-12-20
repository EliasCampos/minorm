import pytest
from decimal import Decimal
from datetime import datetime

from minorm.fields import Field, AutoField, CharField, DecimalField, DateTimeField, ForeignKey, IntegerField


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


class TestIntegerField:

    def test_render_sql_type(self):
        int_field = IntegerField()
        assert int_field.render_sql_type() == 'INTEGER'


class TestCharField:

    def test_render_sql_type(self):
        char_field = CharField(max_length=100)
        assert char_field.render_sql_type() == 'VARCHAR(100)'

    @pytest.mark.parametrize(
        'value, expected', [
            pytest.param("Foo bar", 'Foo bar', id='string-value'),
            pytest.param(b'test', 'test', id='bytes-value'),
            pytest.param(42, '42', id='int-value'),
        ]
    )
    def test_adapt(self, value, expected):
        char_field = CharField(max_length=255)
        assert char_field.adapt(value) == expected


class TestDecimalField:

    def test_render_sql_type(self):
        decimal_field = DecimalField(max_digits=4, decimal_places=2)
        assert decimal_field.render_sql_type() == 'DECIMAL(4, 2)'

    def test_adapt(self):
        val = 4.445

        decimal_field = DecimalField(max_digits=6, decimal_places=2)
        assert decimal_field.adapt(val) == Decimal('4.44')


class TestDateTimeField:

    def test_render_sql_type(self):
        date_field = DateTimeField()
        assert date_field.render_sql_type() == 'DATETIME'

    def test_adapt(self):
        val = '2020-09-02'

        date_field = DateTimeField()
        assert date_field.adapt(val) == datetime(2020, 9, 2)


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
