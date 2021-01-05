from collections import deque, OrderedDict
from itertools import chain
import functools
import operator

from minorm.exceptions import MultipleQueryResult
from minorm.expressions import JoinExpression, OrderByExpression, WhereCondition
from minorm.fields import ForeignKey
from minorm.queries import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery


class RelationNode:

    def __init__(self, base_model, field=None):
        self.model = base_model
        self.field = field
        self.relations = OrderedDict()

    def update(self, lookup_parts):
        field_name, *rest_lookup_parts = lookup_parts
        if field_name not in self.relations:
            field = self.model._meta.get_fk_field(field_name)
            self.relations[field_name] = RelationNode(base_model=field.to, field=field)

        if rest_lookup_parts:
            return self.relations[field_name].update(rest_lookup_parts)
        return self.relations[field_name]


class QuerySet:

    def __init__(self, model):
        self.model = model

        self._where = None
        self._order_by = set()
        self._limit = None

        self._related = RelationNode(base_model=self.model)
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
            field = self.model._meta.check_field(order_exp.value, with_pk=True)
            self._order_by.add(OrderByExpression(value=field.query_name, ordering=order_exp.ordering))
        return self

    def select_related(self, *args):
        if len(args) == 1 and args[0] is None:
            self._related = RelationNode(base_model=self.model)
            return self

        for lookup in args:
            lookup_parts = lookup.split('__')
            self._related.update(lookup_parts)

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
            print(self.query.render_sql(self.model._meta.db.spec))
            return self._dict_from_row(results[0])
        return self._instance_from_row(results[0])

    def all(self):
        extracted = self._extract()
        if self._values_mapping:
            return [self._dict_from_row(row) for row in extracted]
        return [self._instance_from_row(row) for row in extracted]

    def values(self, *args):
        if len(args) == 1 and args[0] is None:
            self._values_mapping = {}
            return self

        for lookup in args:
            val_parts = lookup.split('__')
            *relation_lookup, field_name = val_parts
            if relation_lookup:
                relation = self._related.update(relation_lookup)
            else:
                relation = self._related

            field = relation.model._meta.check_field(field_name, with_pk=True)
            if relation not in self._values_mapping:
                self._values_mapping[relation] = []
            self._values_mapping[relation].append((lookup, field))

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
        joins = self._generate_joins()
        query = (SelectQuery(table_name=self.model._meta.table_name, fields=self._get_all_column_names())
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
            field_name, lookup = WhereCondition.resolve_lookup(key)
            field = self.model._meta.check_field(field_name, with_pk=True)
            adopted_value = field.to_query_parameter(value)

            where_cond = WhereCondition.for_lookup(field.query_name, lookup, adopted_value)
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

    def _instance_from_row(self, row):
        related_queue = deque([self._related])
        parents = {}

        root_instance = None
        while related_queue:
            left_relation = related_queue.popleft()
            model = left_relation.model
            attr_names = [field.name for field in model._meta.fields]
            kwargs = dict(zip(attr_names, row))
            instance = model(**kwargs)

            if left_relation.field:
                field_name, parent = parents[left_relation.field]
                setattr(parent, field_name, instance)
            else:  # in case of base relation node, base on queryset model
                root_instance = instance

            row = row[len(kwargs):]

            for field_name, right_relation in left_relation.relations.items():
                related_queue.append(right_relation)
                fk = right_relation.field
                parents[fk] = (field_name, instance)

        return root_instance

    def _dict_from_row(self, row):
        related_queue = deque([self._related])
        result = {}

        while related_queue:
            relation = related_queue.popleft()
            if relation in self._values_mapping:
                val_names = [lookup for lookup, __ in self._values_mapping[relation]]
                related_values = dict(zip(val_names, row))
                result.update(**related_values)
                row = row[len(related_values):]

            for next_relation in relation.relations.values():
                related_queue.append(next_relation)

        return result

    def _get_all_column_names(self):
        relation_counter = 0
        related_queue = deque([self._related])
        name_shortcuts = {}
        column_names_seq = []
        while related_queue:
            relation = related_queue.popleft()
            if relation.field:
                field_prefix = name_shortcuts[relation.field]
            else:  # in case of base relation node, base on queryset model
                field_prefix = relation.model._meta.table_name

            if self._values_mapping:
                fields = [field for __, field in self._values_mapping.get(relation, [])]
            else:
                fields = relation.model._meta.fields
            column_names_seq.extend(f'{field_prefix}.{field.column_name}' for field in fields)

            for next_relation in relation.relations.values():
                related_queue.append(next_relation)
                relation_counter += 1
                name_shortcuts[next_relation.field] = f'T{relation_counter}'  # T1.column, T2.column, etc..

        return column_names_seq

    def _generate_joins(self):
        relation_counter = 0

        related_queue = deque([self._related])
        name_shortcuts = {}
        joins = []
        while related_queue:
            left_relation = related_queue.popleft()
            for field_name, right_relation in left_relation.relations.items():
                related_queue.append(right_relation)
                relation_counter += 1

                fk = right_relation.field
                pk_shortcut_prefix = f'T{relation_counter}'  # INNER JOIN table_A T1
                name_shortcuts[fk] = pk_shortcut_prefix

                if left_relation.field:
                    fk_field_prefix = name_shortcuts[left_relation.field]  # ON T1.b_id = Tk.id
                else:  # in case of base relation node, base on queryset model
                    fk_field_prefix = left_relation.model._meta.table_name  # ON base_model_table.a_id = T1.id

                join = JoinExpression.on_pk(
                    outer_table=f'{fk.to._meta.table_name} {pk_shortcut_prefix}',
                    fk_field=f'{fk_field_prefix}.{fk.column_name}',
                    pk_field=f'{pk_shortcut_prefix}.{fk.to._meta.pk_field.column_name}',
                )
                joins.append(join)

        return joins
