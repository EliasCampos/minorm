import pytest

from minorm.fields import Field


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
        assert non_unique.to_sql_declaration() == 'test INT'
