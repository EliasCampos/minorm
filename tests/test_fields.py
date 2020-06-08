import pytest
from decimal import Decimal
from datetime import datetime

from minorm.fields import Field, CharField, DecimalField, DateTimeField, PrimaryKey, ForeignKey


class TestField:

    def test_column_name(self):
        class Foo:
            bar = Field()

        assert Foo.bar.column_name == 'bar'

    def test_to_sql_declaration(self, mocker):
        mocker.patch.object(Field, 'get_field_type', lambda obj: 'INTEGER')

        unique_field = Field(null=False, unique=True, default=42, column_name='test')
        assert unique_field.to_sql_declaration() == 'test INTEGER NOT NULL UNIQUE DEFAULT 42'

        non_unique = Field(null=True, unique=False, column_name='test', default=None)
        assert non_unique.to_sql_declaration() == 'test INTEGER DEFAULT NULL'


class TestCharField:

    def test_get_field_type(self):
        char_field = CharField(max_length=100)
        assert char_field.get_field_type() == 'VARCHAR(100)'

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


class TestPrimaryKey:

    def test_get_field_type(self):
        pk = PrimaryKey(auto_increment='SERIAL', pk_declaration='INTEGER PRIMARY KEY SERIAL')
        assert pk.get_field_type() == "INTEGER PRIMARY KEY SERIAL"


class TestForeignKey:

    def test_get_field_type(self, test_model):
        fk = ForeignKey(null=False, to=test_model, on_delete=ForeignKey.CASCADE)
        assert fk.get_field_type() == "INTEGER REFERENCES person (id)"

    def test_column_name(self, test_model):
        fk = ForeignKey(null=False, to=test_model, on_delete=ForeignKey.RESTRICT)
        assert fk.column_name == "person_id"


class TestDecimalField:

    def test_adapt(self):
        val = 4.445

        decimal_field = DecimalField(max_digits=6, decimal_places=2)
        assert decimal_field.adapt(val) == Decimal('4.44')


class TestDateTimeField:

    def test_adapt(self):
        val = '2020-09-02'

        time_field = DateTimeField(max_digits=6, decimal_places=2)
        assert time_field.adapt(val) == datetime(2020, 9, 2)
