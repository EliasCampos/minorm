import functools
import operator

from minorm.expressions import OrderByExpression, WhereCondition


class QueryExpression:

    def __init__(self, model):
        self.model = model

        self._where = None
        self._order_by = set()

    def filter(self, **kwargs):
        where_cond = self._where_action(**kwargs)
        self._reset_where(where_cond, operator.and_)
        return self

    def aswell(self, **kwargs):
        where_cond = self._where_action(**kwargs)
        self._reset_where(where_cond, operator.or_)
        return self

    def order_by(self, *args):
        for field_name in args:
            order_exp = OrderByExpression.from_field_name(field_name)
            self.model.check_field(order_exp.value)
            self._order_by.add(order_exp)
        return self

    def _where_action(self, *args, **kwargs):
        where_conds = list(args)

        for key, value in kwargs.items():
            field_name, op = WhereCondition.resolve_lookup(key)
            self.model.check_field(field_name)
            sql_value = self.model.field_to_sql(field_name, value)

            where_cond = WhereCondition(field_name, op, sql_value)
            where_conds.append(where_cond)

        result = functools.reduce(operator.and_, where_conds)
        return result

    def _reset_where(self, where_cond, op):
        if not self._where:
            self._where = where_cond
        else:
            self._where = op(self._where, where_cond)
