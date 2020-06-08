from collections import OrderedDict
from itertools import chain
import functools
import operator

from minorm.exceptions import MultipleQueryResult
from minorm.expressions import JoinExpression, OrderByExpression, WhereCondition
from minorm.fields import ForeignKey
from minorm.queries import InsertQuery, SelectQuery, UpdateQuery


class QuerySet:

    def __init__(self, model):
        self.model = model

        self._where = None
        self._order_by = set()
        self._limit = None

        self._related = {}
        self._values_mapping = {}

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
            field = self.model.check_field(order_exp.value, with_pk=True)
            self._order_by.add(OrderByExpression(value=field.query_name, ordering=order_exp.ordering))
        return self

    def select_related(self, *args):
        if len(args) == 1 and args[0] is None:
            self._related = {}
            return self

        for field_name in args:
            field = self.model.check_field(field_name, with_pk=True)
            if isinstance(field, ForeignKey):
                self._related[field.to] = field

        return self

    def update(self, **kwargs):
        update_data = OrderedDict()

        for key, value in kwargs.items():
            field = self.model.check_field(key, with_pk=False)
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
        self._values_mapping = {}
        self._limit = 2
        results = self._extract(**kwargs)
        if not results:
            raise self.model.DoesNotExists
        if len(results) > 1:
            raise MultipleQueryResult

        return self._instance_from_result(results)

    def first(self):
        self._values_mapping = {}
        self._limit = 1
        results = self._extract()
        if not results:
            return None
        return self._instance_from_result(results)

    def all(self):
        extracted = self._extract()
        if self._values_mapping:
            return [self._dict_from_row(row) for row in extracted]
        return [self.model.instance_from_row(row, related=self._related) for row in extracted]

    def values(self, *args):
        if len(args) == 1 and args[0] is None:
            self._values_mapping = {}
            return self

        for value in args:
            val_parts = value.split('__')
            if len(val_parts) > 1:
                rel = val_parts[0]
                rel_field_name = val_parts[1]
                for fk in self._related.values():
                    if rel == fk.name:
                        rel_field = fk.to.check_field(rel_field_name, with_pk=True)
                        self._values_mapping[value] = rel_field
                        break
                else:
                    raise ValueError(f'{rel} does not belong supported relations.')
            else:
                val = val_parts[0]
                field = self.model.check_field(val, with_pk=True)
                self._values_mapping[val] = field

        return self

    def exists(self):
        self.select_related(None)
        self.values(self.model.PK_FIELD)
        self._limit = 1

        is_exists = bool(self._extract())
        return is_exists

    def bulk_create(self, instances):
        model = self.model

        field_names = model.fields.keys()
        column_names = [field.column_name for field in model.fields.values()]
        values = [[getattr(obj, name) for name in field_names] for obj in instances if isinstance(obj, model)]

        operation = InsertQuery(db=model.db, table_name=model.table_name, fields=column_names)
        operation.execute_many(params=values)

        return model.db.last_query_rowcount

    @property
    def query(self):
        joins = [JoinExpression.on_pk(fld.to.table_name, fld.query_name, fld.to.pk_query_name)
                 for fld in self._related.values()]
        query = (SelectQuery(db=self.model.db, table_name=self.model.table_name, fields=self._get_fields())
                 .join(joins)
                 .where(self._where)
                 .limit(self._limit)
                 .order_by(self._order_by))
        return query

    @property
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
            field = self.model.check_field(field_name, with_pk=True)
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

        select_query = self.query
        params = self.query_params
        results = select_query.execute(params=params)
        return results

    def _dict_from_row(self, row):
        return {val: row[f'{field.model.name}_{field.name}'] for val, field in self._values_mapping.items()}

    def _check_pk_lookups(self, kwargs):
        conds = []
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
            del kwargs[lookup]

        return conds

    def _instance_from_result(self, results):
        row = results[0]
        instance = self.model.instance_from_row(row, related=self._related, is_tuple=False)
        return instance

    def _get_fields(self):
        if self._values_mapping:
            field_seq = [field.select_field_name for field in self._values_mapping.values()]
        else:
            all_models = (self.model, ) + tuple(self._related)
            field_seq = chain.from_iterable(mdl.select_field_names for mdl in all_models)
        return tuple(field_seq)
