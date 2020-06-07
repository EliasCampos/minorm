import pytest

from minorm.expressions import WhereCondition


class TestWhereCondition:

    def test_str(self):
        where_cond = WhereCondition(field='x', op='=', value='3')

        assert str(where_cond) == "x = 3"

    def test_and(self):
        where_cond1 = WhereCondition(field='x', op='=', value='3')
        where_cond2 = WhereCondition(field='y', op='=', value='5')

        result = where_cond1 & where_cond2
        assert result is where_cond1
        assert result._and is where_cond2

        assert str(result) == "x = 3 AND y = 5"

    def test_or(self):
        where_cond1 = WhereCondition(field='x', op='=', value='3')
        where_cond2 = WhereCondition(field='y', op='=', value='5')

        result = where_cond1 | where_cond2
        assert result is where_cond1
        assert result._or is where_cond2

        assert str(result) == "x = 3 OR y = 5"

    def test_or_and(self):
        where_cond1 = WhereCondition(field='x', op='=', value='3')
        where_cond2 = WhereCondition(field='y', op='=', value='5')
        where_cond3 = WhereCondition(field='z', op='=', value='42')

        assert str(where_cond3 | where_cond1 & where_cond2) == "z = 42 OR x = 3 AND y = 5"

    def test__not(self):
        where_cond = WhereCondition(field='x', op='=', value='3')
        assert not where_cond._negated

        where_cond2 = ~where_cond
        assert where_cond2._negated

        assert str(where_cond2) == "NOT (x = 3)"

    def test_not_and(self):
        where_cond1 = WhereCondition(field='x', op='=', value='3')
        where_cond2 = WhereCondition(field='y', op='=', value='5')

        assert str(~(where_cond1 & where_cond2)) == "NOT (x = 3 AND y = 5)"
