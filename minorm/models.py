from collections import namedtuple

from minorm.connectors import connector
from minorm.exceptions import DoesNotExists
from minorm.expressions import WhereCondition
from minorm.fields import AutoField, Field, ForeignKey
from minorm.managers import QuerySet
from minorm.queries import CreateTableQuery, DeleteQuery, DropTableQuery, InsertQuery, UpdateQuery, SelectQuery


class ModelSetupError(Exception):
    pass


class ModelMetaData:

    def __init__(self, model_name, db, table_name, fields):
        self._model_name = model_name
        self._db = db
        self._table_name = table_name
        self._fields = fields

    @property
    def db(self):
        return self._db

    @property
    def table_name(self):
        return self._table_name

    @property
    def fields(self):
        return list(self._fields)

    @property
    def pk_field(self):
        return next((field for field in self.fields if field.is_pk))

    @property
    def name(self):
        return self._model_name.lower()

    @property
    def column_names(self):
        return [field.column_name for field in self.fields]

    @property
    def query_names(self):
        return [field.query_name for field in self.fields]

    def check_field(self, field_name, with_pk=False):
        for field in self.fields:
            if field.name == field_name and (not field.is_pk or with_pk):
                return field

        raise ValueError(f'{field_name} is not a valid field for model {self._model_name}.')

    def get_field(self, field_name):
        return self.check_field(field_name, with_pk=True)

    def get_fk_field(self, field_name):
        for field in self.fields:
            if field.name == field_name and isinstance(field, ForeignKey):
                return field

        raise ValueError(f'{field_name} is not a valid foreign relation for model {self._model_name}.')


class ModelMetaclass(type):

    def __new__(mcs, name, bases, namespace):
        if not bases:
            return super().__new__(mcs, name, bases, namespace)

        fields = [class_attr for class_attr in namespace.values() if isinstance(class_attr, Field)]
        # Extract model meta
        meta = namespace.pop('Meta', None)
        table_name = getattr(meta, 'table_name', name.lower())
        db = getattr(meta, 'db', connector)

        queryset_class = namespace.pop('queryset_class', QuerySet)

        # Extract primary key:
        pk_fields = [field for field in fields if field.is_pk]
        if len(pk_fields) > 1:
            raise ModelSetupError('Model should have only one primary key.')
        if pk_fields:
            pk_field = pk_fields[0]
        else:
            pk_field = AutoField(pk=True, column_name='id')
            setattr(pk_field, '_name', 'id')
        fields.append(pk_field)

        # Setup model pk, meta and queryset attributes:
        model = super().__new__(mcs, name, bases, namespace)

        if not pk_field.model:
            setattr(pk_field, '_model', model)

        setattr(model, '_meta', ModelMetaData(model_name=model.__name__, db=db, table_name=table_name, fields=fields))

        setattr(model, '_queryset_class', queryset_class)

        does_not_exists = type(f'{model.__name__}DoesNotExists', (DoesNotExists,), {})
        setattr(model, 'DoesNotExists', does_not_exists)

        query_namedtuple = namedtuple(f'{model.__name__}QueryNamedTuple', field_names=[field.name for field in fields])
        setattr(model, 'query_namedtuple', query_namedtuple)

        return model

    @property
    def qs(cls):
        return cls._queryset_class(model=cls)

    def render_sql(cls):
        field_params = [field.render_sql() for field in cls._meta.fields]
        create_query = CreateTableQuery(table_name=cls._meta.table_name, params=field_params)
        return create_query.render_sql()

    def create_table(cls):
        raw_sql = cls.render_sql()
        with cls._meta.db.cursor() as curr:
            curr.execute(raw_sql)

    def drop_table(cls):
        drop_query = DropTableQuery(table_name=cls._meta.table_name)
        raw_sql = drop_query.render_sql()
        with cls._meta.db.cursor() as curr:
            curr.execute(raw_sql)


class Model(metaclass=ModelMetaclass):

    def __init__(self, **kwargs):
        for field in self.__class__._meta.fields:
            field_name = field.name
            value = kwargs.get(field_name, field.get_default())
            setattr(self, field_name, value)

    @property
    def pk(self):
        return getattr(self, self.__class__._meta.pk_field.name)

    def save(self):
        is_creation = not bool(self.pk)
        self._adapt_values()

        model = self.__class__

        modified_fields = [field for field in model._meta.fields if not isinstance(field, AutoField)]
        if is_creation:
            query_class = InsertQuery
            where_cond = None
        else:
            query_class = UpdateQuery
            where_cond = WhereCondition(model._meta.pk_field.query_name, WhereCondition.EQ, self.pk)

        query = query_class(
            table_name=model._meta.table_name,
            fields=[field.column_name for field in modified_fields],
            where=where_cond,
        )
        raw_sql = query.render_sql(model._meta.db.spec)
        query_params = [getattr(self, field.name) for field in modified_fields]
        if where_cond:
            query_params.extend(where_cond.values())
        with model._meta.db.cursor() as curr:
            curr.execute(raw_sql, query_params)
        if is_creation:
            setattr(self, model._meta.pk_field.name, curr.lastrowid)

    def _adapt_values(self):
        for field in self.__class__._meta.fields:
            field_name = field.name
            adapted_value = field.adapt_value(getattr(self, field_name))
            setattr(self, field_name, adapted_value)

    def refresh_from_db(self):
        if not self.pk:
            return

        model = self.__class__

        pk_cond = WhereCondition(model._meta.pk_field.query_name, WhereCondition.EQ, self.pk)
        query_fields = [field.query_name for field in model._meta.fields]
        select_query = SelectQuery(table_name=model._meta.table_name, fields=query_fields, where=pk_cond)
        raw_sql = select_query.render_sql(model._meta.db.spec)
        params = pk_cond.values()
        with model._meta.db.cursor() as curr:
            curr.execute(raw_sql, params)
            row = curr.fetchone()
        for i, field in enumerate(model._meta.fields):
            setattr(self, field.name, row[i])

    def delete(self):
        if not self.pk:
            return

        model = self.__class__
        pk_cond = WhereCondition(model._meta.pk_field.query_name, WhereCondition.EQ, self.pk)
        delete_query = DeleteQuery(table_name=model._meta.table_name, where=pk_cond)

        raw_sql = delete_query.render_sql(model._meta.db.spec)
        with model._meta.db.cursor() as curr:
            curr.execute(raw_sql, pk_cond.values())

        setattr(self, model._meta.pk_field.name, None)
        return curr.rowcount
