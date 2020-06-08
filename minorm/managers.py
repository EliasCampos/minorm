from collections import OrderedDict
from itertools import chain
import functools
import operator

from minorm.exceptions import MultipleQueryResult
from minorm.expressions import JoinExpression, OrderByExpression, WhereCondition
from minorm.fields import ForeignKey
from minorm.queries import InsertQuery, SelectQuery, UpdateQuery


class QueryExpression:

    def __init__(self, model):
        self.model = model

        self._where = None
        self._order_by = set()
        self._limit = None

        self._related = {}

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

    def select_related(self, *args):
        for field_name in args:
            self.model.check_field(field_name)
            field = self.model.fields[field_name]
            if isinstance(field, ForeignKey):
                self._related[field.to] = field

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
        self._limit = 2
        results = self._extract(**kwargs)
        if not results:
            raise self.model.DoesNotExists
        if len(results) > 1:
            raise MultipleQueryResult

        return self._instance_from_result(results)

    def first(self):
        self._limit = 1
        results = self._extract()
        if not results:
            return None
        return self._instance_from_result(results)

    def all(self):
        return [self.model.instance_from_row(row, related=self._related) for row in self._extract()]

    def bulk_create(self, instances):
        model = self.model

        field_names = model.fields.keys()
        column_names = [field.column_name for field in model.fields.values()]
        values = [[getattr(obj, name) for name in field_names] for obj in instances if isinstance(obj, model)]

        operation = InsertQuery(db=model.db, table_name=model.table_name, fields=column_names)
        operation.execute_many(params=values)

        return model.db.last_query_rowcount

    def query(self):
        self_pk = self.model.pk_query_name
        joins = [JoinExpression.on_pk(fld.to.table_name, self_pk, fld.query_name) for fld in self._related.values()]

        all_models = (self.model, ) + tuple(self._related)
        fields = tuple(chain.from_iterable(mdl.select_field_names for mdl in all_models))
        query = (SelectQuery(db=self.model.db, table_name=self.model.table_name, fields=fields)
                 .join(joins)
                 .where(self._where)
                 .limit(self._limit)
                 .order_by(self._order_by))
        return query

    def query_params(self):
        return self._where.values() if self._where else ()

    def __getitem__(self, item):
        if not any(isinstance(item, supported_type) for supported_type in (int, slice)):
            raise TypeError(f'{self.__class__.__name__} indices must be integers or slices.')

        if isinstance(item, slice):
            self._limit = item.stop
            return self

        return self.all()[item]

    def _where_action(self, *args, **kwargs):
        where_conds = self._check_pk_lookups(kwargs)
        where_conds.extend(args)

        for key, value in kwargs.items():
            field_name, op = WhereCondition.resolve_lookup(key)
            self.model.check_field(field_name)
            field = self.model.fields[field_name]
            adopted_value = field.to_query_parameter(value)

            where_cond = WhereCondition(field.query_name, op, adopted_value)
            where_conds.append(where_cond)

        result = functools.reduce(operator.and_, where_conds) if where_conds else None
        return result

    def _reset_where(self, where_cond, op):
        if not self._where:
            self._where = where_cond
        else:
            self._where = op(self._where, where_cond)

    def _extract(self, **kwargs):
        where_cond = self._where_action(**kwargs)
        self._reset_where(where_cond, operator.and_)

        select_query = self.query()
        params = self.query_params()
        results = select_query.execute(params=params)
        return results

    def _check_pk_lookups(self, kwargs):
        pk = kwargs.pop(self.model.PK_FIELD, None)

        conds = []

        if pk is not None:
            pk_cond = WhereCondition(self.model.pk_query_name, WhereCondition.EQ, pk)
            conds.append(pk_cond)

        lookups = set()
        pk_prefix = f'{self.model.PK_FIELD}__'

        for key, value in kwargs.items():
            if not key.startswith(pk_prefix):
                continue

            _, op = WhereCondition.resolve_lookup(key)
            where_cond = WhereCondition(self.model.pk_query_name, op, value)
            conds.append(where_cond)
            lookups.add(key)

        for lookup in lookups:
            kwargs.pop(lookup)

        return conds

    def _instance_from_result(self, results):
        row = results[0]
        instance = self.model.instance_from_row(row, related=self._related, is_tuple=False)
        return instance
