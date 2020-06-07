from collections import OrderedDict
import functools
import operator

from minorm.exceptions import MultipleQueryResult
from minorm.expressions import OrderByExpression, WhereCondition
from minorm.queries import UpdateQuery, SelectQuery


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

    def update(self, **kwargs):
        update_data = OrderedDict()

        for key, value in kwargs.items():
            self.model.check_field(key)
            field = self.model.fields[key]
            adopted_value = field.adapt(value)
            update_data[field.column_name] = adopted_value

        update_query = UpdateQuery(db=self.model.db, table_name=self.model.table_name, fields=update_data.keys(),
                                   where=self._where)
        params = tuple(update_data.values()) + (self._where.values() if self._where else ())
        update_query.execute(params=params)

    def create(self, **kwargs):
        instance = self.model(**kwargs)
        instance.save()
        return instance

    def get(self, **kwargs):
        results = self._extract(**kwargs)
        if not results:
            raise self.model.DoesNotExists
        if len(results) > 1:
            raise MultipleQueryResult

        return self._instance_from_result(results)

    def first(self):
        results = self._extract()
        if not results:
            return None
        return self._instance_from_result(results)

    def _where_action(self, *args, **kwargs):
        where_conds = list(args)

        for key, value in kwargs.items():
            field_name, op = WhereCondition.resolve_lookup(key)
            self.model.check_field(field_name)
            field = self.model.fields[field_name]
            adopted_value = field.to_query_parameter(value)

            where_cond = WhereCondition(field.column_name, op, adopted_value, val_place=self.model.db.VAL_PLACE)
            where_conds.append(where_cond)

        result = functools.reduce(operator.and_, where_conds) if where_conds else None
        return result

    def _reset_where(self, where_cond, op):
        if not self._where:
            self._where = where_cond
        else:
            self._where = op(self._where, where_cond)

    def _extract(self, **kwargs):
        pk_field = self.model.PK_FIELD
        pk = kwargs.pop(pk_field, None)
        where_cond = self._where_action(**kwargs)
        if pk is not None:
            pk_cond = WhereCondition(pk_field, WhereCondition.EQ, pk, val_place=self.model.db.VAL_PLACE)
            if where_cond:
                where_cond &= pk_cond
            else:
                where_cond = pk_cond

        self._reset_where(where_cond, operator.and_)

        fields = [pk_field] + [field.column_name for field in self.model.fields.values()]
        select_query = SelectQuery(db=self.model.db, table_name=self.model.table_name, fields=fields,
                                   where=self._where, order_by=self._order_by)

        params = self._where.values() if self._where else ()
        results = select_query.execute(params=params)
        return results

    def _instance_from_result(self, results):
        result = results[0]
        pk_field = self.model.PK_FIELD
        fields = [pk_field] + list(self.model.fields.keys())

        extracted = {name: val for name, val in zip(fields, result)}
        pk_val = extracted.pop(pk_field)
        instance = self.model(**extracted)
        setattr(instance, pk_field, pk_val)
        return instance
