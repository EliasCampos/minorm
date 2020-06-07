import pytest

from minorm.fields import Field, CharField


class TestField:

    def test_column_name(self):
        class Foo:
            bar = Field()

        assert Foo.bar.column_name == 'bar'

    def test_to_sql_declaration(self, mocker):
        mocker.patch.object(Field, 'get_field_type', lambda obj: 'INT')

        unique_field = Field(null=False, unique=True, default=42, column_name='test')
        assert unique_field.to_sql_declaration() == 'test INT NOT NULL UNIQUE DEFAULT 42'

        non_unique = Field(null=True, unique=False, column_name='test')
        assert non_unique.to_sql_declaration() == 'test INT DEFAULT NULL'


class TestCharField:

    def test_get_field_type(self):
        char_field = CharField(max_length=100)
        assert char_field.get_field_type() == 'VARCHAR(100)'

    @pytest.mark.parametrize(
        'value, expected', [
            pytest.param("Foo bar", 'Foo bar', id='string-value'),
            pytest.param(b'test', 'test', id='bytes-value'),
            pytest.param(42, '42', id='integer-value'),
        ]
    )
    def test_adapt(self, value, expected):
        char_field = CharField(max_length=255)
        assert char_field.adapt(value) == expected
