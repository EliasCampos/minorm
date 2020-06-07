import pytest

from minorm.expressions import WhereCondition, OrderByExpression


class TestWhereCondition:

    def test_str(self):
        where_cond = WhereCondition(field='x', op='=', value='3')

        assert str(where_cond) == "x = ?"

    def test_and(self):
        where_cond1 = WhereCondition(field='x', op='=', value='3')
        where_cond2 = WhereCondition(field='y', op='=', value='5')

        result = where_cond1 & where_cond2
        assert result is where_cond1
        assert result._and is where_cond2

        assert str(result) == "x = ? AND y = ?"
        assert result.values() == ('3', '5')

    def test_or(self):
        where_cond1 = WhereCondition(field='x', op='=', value='3')
        where_cond2 = WhereCondition(field='y', op='=', value='5')

        result = where_cond1 | where_cond2
        assert result is where_cond1
        assert result._or is where_cond2

        assert str(result) == "x = ? OR y = ?"
        assert result.values() == ('3', '5')

    def test_or_and(self):
        where_cond1 = WhereCondition(field='x', op='=', value='3')
        where_cond2 = WhereCondition(field='y', op='=', value='5')
        where_cond3 = WhereCondition(field='z', op='=', value='42')

        assert str(where_cond3 | where_cond1 & where_cond2) == "z = ? OR x = ? AND y = ?"
        assert (where_cond3 | where_cond1 & where_cond2).values() == ('42', '3', '5')

    def test__not(self):
        where_cond = WhereCondition(field='x', op='=', value='3')
        assert not where_cond._negated

        where_cond2 = ~where_cond
        assert where_cond2._negated

        assert str(where_cond2) == "NOT (x = ?)"
        assert where_cond2.values() == ('3', )

    def test_not_and(self):
        where_cond1 = WhereCondition(field='x', op='=', value='3')
        where_cond2 = WhereCondition(field='y', op='=', value='5')

        not_and = ~(where_cond1 & where_cond2)
        assert str(not_and) == "NOT (x = ? AND y = ?)"
        assert not_and.values() == ('3', '5')

    @pytest.mark.parametrize(
        'lookup, expected_op', [
            ('lt', '<'),
            ('lte', '<='),
            ('gt', '>'),
            ('gte', '>='),
            ('in', 'IN'),
            ('neq', '!='),
        ]
    )
    def test_resolve_lookup(self, lookup, expected_op):
        field = f'test__{lookup}'
        assert WhereCondition.resolve_lookup(field) == ('test', expected_op)

    def test_resolve_lookup_eq(self):
        field = f'test'
        assert WhereCondition.resolve_lookup(field) == ('test', '=')

    def test_resolve_lookup_value_error(self):
        invalid_lookup = 'foobar'
        with pytest.raises(ValueError, match=r'.*lookup.*'):
            WhereCondition.resolve_lookup(f'test__{invalid_lookup}')


class TestOrderByExpression:

    def test_from_field_name(self):
        field1 = 'foo'
        expr = OrderByExpression.from_field_name(field1)
        assert expr.value == 'foo'
        assert expr.ordering == 'ASC'

        field2 = '-bar'
        expr = OrderByExpression.from_field_name(field2)
        assert expr.value == 'bar'
        assert expr.ordering == 'DESC'

    @pytest.mark.parametrize(
        'value, ordering, expected_string', [
            ['foo', OrderByExpression.ASC, 'foo ASC'],
            ['bar', OrderByExpression.DESC, 'bar DESC'],
        ]
    )
    def test_str(self, value, ordering, expected_string):
        expr = OrderByExpression(value=value, ordering=ordering)
        assert str(expr) == expected_string
