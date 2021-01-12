from collections import OrderedDict
import functools
import operator

from minorm.exceptions import MultipleQueryResult
from minorm.expressions import JoinExpression, LOOKUP_SEPARATOR, OrderByExpression, WhereCondition
from minorm.queries import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery


class RelationNode:
    """A helper class for constructing nested foreign relations."""

    def __init__(self, base_model, depth=0, position=0):
        self.model = base_model

        self.depth = depth
        self.position = position

        self.is_selected = False  # should be True when the relation is marked in `select_related` method

        self.relations = OrderedDict()

    def resolve_relation(self, lookup_parts, is_selected=False):
        field_name, *rest_lookup_parts = lookup_parts
        if field_name not in self.relations:
            field = self.model._meta.get_fk_field(field_name)
            position = len(self.relations) + 1
            self.relations[field_name] = RelationNode(base_model=field.to, depth=self.depth + 1, position=position)

        if is_selected:
            self.relations[field_name].is_selected = True

        if rest_lookup_parts:
            return self.relations[field_name].resolve_relation(rest_lookup_parts, is_selected=is_selected)
        return self.relations[field_name]

    @property
    def is_root_node(self):
        return self.depth == self.position == 0

    @property
    def table_shortcut(self):
        if self.is_root_node:
            return self.model._meta.table_name

        return f'T{self.depth}{self.position}'

    @property
    def table_name(self):
        if self.is_root_node:
            return self.model._meta.table_name

        return f'{self.model._meta.table_name} {self.table_shortcut}'

    def get_column_names(self):
        column_names = [f'{self.table_shortcut}.{field.column_name}' for field in self.model._meta.fields]
        for rel in self.relations.values():
            if rel.is_selected:
                column_names.extend(rel.get_column_names())
        return column_names

    def get_joins(self):
        joins = []
        for attr_name, rel in self.relations.items():
            fk = self.model._meta.get_fk_field(attr_name)
            joins.append(
                JoinExpression.on_pk(
                    outer_table=rel.table_name,
                    fk_field=f'{self.table_shortcut}.{fk.column_name}',
                    pk_field=f'{rel.table_shortcut}.{fk.to._meta.pk_field.column_name}',
                )
            )
            joins.extend(rel.get_joins())

        return joins

    def row_to_instance(self, row, is_namedtuple, row_shift=0):
        model = self.model

        row_part = row[row_shift:]
        attr_names = (field.name for field in model._meta.fields)
        kwargs = dict(zip(attr_names, row_part))
        row_shift += len(kwargs)

        for fk_name, rel in self.relations.items():
            if rel.is_selected:
                kwargs[fk_name], row_shift = rel.row_to_instance(row, is_namedtuple, row_shift=row_shift)

        klass = model.query_namedtuple if is_namedtuple else model
        return klass(**kwargs), row_shift


class QuerySet:

    def __init__(self, model):
        self.model = model

        self._where = None
        self._order_by = set()
        self._limit = None

        self._related = RelationNode(base_model=self.model)
        self._values_mapping = OrderedDict()

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
            field = self.model._meta.check_field(order_exp.value, with_pk=True)
            self._order_by.add(OrderByExpression(value=field.query_name, ordering=order_exp.ordering))
        return self

    def select_related(self, *args):
        if len(args) == 1 and args[0] is None:
            self._related = RelationNode(base_model=self.model)
            return self

        for lookup in args:
            lookup_parts = lookup.split(LOOKUP_SEPARATOR)
            self._related.resolve_relation(lookup_parts, is_selected=True)

        return self

    def update(self, **kwargs):
        update_data = OrderedDict()
        for key, value in kwargs.items():
            field = self.model._meta.check_field(key, with_pk=False)
            adopted_value = field.adapt_value(value)
            update_data[field.column_name] = adopted_value

        update_query = UpdateQuery(
            table_name=self.model._meta.table_name,
            fields=update_data.keys(),
            where=self._where,
        )
        raw_sql = update_query.render_sql(self.model._meta.db.spec)
        params = tuple(update_data.values()) + (self._where.values() if self._where else ())
        with self.model._meta.db.cursor() as curr:
            curr.execute(raw_sql, params)
        return curr.rowcount

    def delete(self):
        delete_query = DeleteQuery(table_name=self.model._meta.table_name, where=self._where)
        raw_sql = delete_query.render_sql(self.model._meta.db.spec)

        with self.model._meta.db.cursor() as curr:
            curr.execute(raw_sql, self.query_params)
        return curr.rowcount

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

        return self._instance_from_row(results[0])

    def first(self):
        self._limit = 1
        results = self._extract()
        if not results:
            return None

        if self._values_mapping:
            return self._dict_from_row(results[0])
        return self._instance_from_row(results[0])

    def all(self):
        extracted = self._extract()
        if self._values_mapping:
            return [self._dict_from_row(row) for row in extracted]
        return [self._instance_from_row(row, is_namedtuple=True) for row in extracted]

    def values(self, *args):
        if len(args) == 1 and args[0] is None:
            self._values_mapping = OrderedDict()
            return self

        for lookup in args:
            val_parts = lookup.split(LOOKUP_SEPARATOR)
            *relation_lookup, field_name = val_parts
            if relation_lookup:
                relation = self._related.resolve_relation(relation_lookup)
            else:
                relation = self._related

            field = relation.model._meta.check_field(field_name, with_pk=True)
            self._values_mapping[lookup] = f'{relation.table_shortcut}.{field.column_name}'

        return self

    def exists(self):
        self.select_related(None)
        self.values(self.model._meta.pk_field.column_name)
        self._limit = 1

        is_exists = bool(self._extract())
        return is_exists

    def bulk_create(self, instances):
        model = self.model
        fields = model._meta.fields
        db = model._meta.db

        insert_query = InsertQuery(table_name=model._meta.table_name, fields=[field.column_name for field in fields])
        raw_sql = insert_query.render_sql(db.spec)
        params = [[getattr(obj, field.name) for field in fields] for obj in instances if isinstance(obj, model)]
        with db.cursor() as curr:
            curr.executemany(raw_sql, params)
        return curr.rowcount

    @property
    def query(self):
        column_names = self._values_mapping.values() if self._values_mapping else self._related.get_column_names()
        joins = self._related.get_joins()

        query = (SelectQuery(table_name=self._related.table_name, fields=column_names)
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
            field_part, lookup = WhereCondition.resolve_lookup(key)
            *rel_lookups, field_name = field_part.split(LOOKUP_SEPARATOR)
            if rel_lookups:
                related = self._related.resolve_relation(rel_lookups)
            else:
                related = self._related

            field = related.model._meta.check_field(field_name, with_pk=True)
            adopted_value = field.to_query_parameter(value)
            where_cond = WhereCondition.for_lookup(
                f'{related.table_shortcut}.{field.column_name}', lookup, adopted_value
            )
            where_conds.append(where_cond)

        result = functools.reduce(operator.and_, where_conds) if where_conds else None
        return result

    def _reset_where(self, where_cond, op):
        if not where_cond:
            return

        if not self._where:
            self._where = where_cond
        else:
            self._where = op(self._where, where_cond)

    def _extract(self, **kwargs):
        where_cond = self._where_action(**kwargs)
        self._reset_where(where_cond, operator.and_)

        select_query = self.query
        params = self.query_params

        raw_sql = select_query.render_sql(self.model._meta.db.spec)

        with self.model._meta.db.cursor() as curr:
            curr.execute(raw_sql, params)
            results = curr.fetchall()
        return results

    def _check_pk_lookups(self, kwargs):
        conds = []
        lookups = set()

        pk_prefix = f'{self.model._meta.pk_field.name}__'
        for key, value in kwargs.items():
            if not key.startswith(pk_prefix):
                continue

            _, lookup = WhereCondition.resolve_lookup(key)
            where_cond = WhereCondition.for_lookup(self.model._meta.pk_field.query_name, lookup, value)
            conds.append(where_cond)
            lookups.add(key)

        for lookup in lookups:
            del kwargs[lookup]

        return conds

    def _instance_from_row(self, row, is_namedtuple=False):
        instance, __ = self._related.row_to_instance(row, is_namedtuple=is_namedtuple)
        return instance

    def _dict_from_row(self, row):
        return dict(zip(self._values_mapping, row))
