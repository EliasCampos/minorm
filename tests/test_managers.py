import pytest

from minorm.managers import QueryExpression, OrderByExpression


class FakeModel:

    @staticmethod
    def check_field(field_name):
        pass

    @staticmethod
    def field_to_sql(field_name, field_value):
        return str(field_value)


@pytest.fixture(scope="function")
def fake_model():
    return FakeModel()


class TestQueryExpression:

    def test_filter(self, fake_model):
        query = QueryExpression(model=fake_model)

        result = query.filter(x='1', y__gt='2')

        assert result is query
        assert str(result._where) == "x = 1 AND y > 2"

    def test_aswell(self, fake_model):
        query = QueryExpression(model=fake_model)

        result = query.filter(x__in=(3, 4)).aswell(y__lte=5)

        assert result is query
        assert str(result._where) == "x IN (3, 4) OR y <= 5"

    def test_order_by(self, fake_model):
        query = QueryExpression(model=fake_model)

        result = query.order_by('foo', '-bar')

        assert result is query
        assert result._order_by == {OrderByExpression('foo', 'ASC'), OrderByExpression('bar', 'DESC')}
