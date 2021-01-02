from collections import namedtuple

from minorm.connectors import connector
from minorm.exceptions import DoesNotExists
from minorm.expressions import WhereCondition
from minorm.fields import AutoField, Field
from minorm.managers import QuerySet
from minorm.queries import CreateTableQuery, DeleteQuery, DropTableQuery, InsertQuery, UpdateQuery, SelectQuery


model_metadata = namedtuple('Meta', 'db, table_name')


class ModelSetupError(Exception):
    pass


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
        setattr(model, '_fields', fields)
        if not pk_field.model:
            setattr(pk_field, '_model', model)

        setattr(model, '_meta', model_metadata(db=db, table_name=table_name))

        setattr(model, '_queryset_class', queryset_class)

        does_not_exists = type(f'{model.__name__}DoesNotExists', (DoesNotExists,), {})
        setattr(model, 'DoesNotExists', does_not_exists)

        query_namedtuple = namedtuple(f'{model.__name__}QueryNamedTuple',
                                      field_names=[field.name for field in model.fields])
        setattr(model, 'query_namedtuple', query_namedtuple)

        return model

    @property
    def qs(cls):
        return cls._queryset_class(model=cls)

    @property
    def fields(cls):
        return tuple(cls._fields)

    @property
    def pk_field(cls):
        return next((field for field in cls.fields if field.is_pk))

    @property
    def db(cls):
        return cls._meta.db

    @property
    def table_name(cls):
        return cls._meta.table_name

    @property
    def name(cls):
        return cls.__name__.lower()

    @property
    def column_names(cls):
        return [field.column_name for field in cls.fields]

    @property
    def query_names(cls):
        return [field.query_name for field in cls.fields]

    def render_sql(cls):
        field_params = [field.render_sql() for field in cls.fields]
        create_query = CreateTableQuery(table_name=cls.table_name, params=field_params)
        return create_query.render_sql()

    def create_table(cls):
        raw_sql = cls.render_sql()
        with cls.db.cursor() as curr:
            curr.execute(raw_sql)

    def drop_table(cls):
        drop_query = DropTableQuery(table_name=cls.table_name)
        raw_sql = drop_query.render_sql()
        with cls.db.cursor() as curr:
            curr.execute(raw_sql)

    def check_field(cls, field_name, with_pk=False):
        for field in cls.fields:
            if field.name == field_name and (not field.is_pk or with_pk):
                return field

        raise ValueError(f'{field_name} is not a valid field for model {cls.__name__}.')


class Model(metaclass=ModelMetaclass):

    def __init__(self, **kwargs):
        for field in self.__class__.fields:
            field_name = field.name
            value = kwargs.get(field_name, field.get_default())
            setattr(self, field_name, value)

    @property
    def pk(self):
        return getattr(self, self.__class__.pk_field.name)

    def save(self):
        is_creation = not bool(self.pk)
        self._adapt_values()

        model = self.__class__

        modified_fields = [field for field in model.fields if not isinstance(field, AutoField)]
        if is_creation:
            query_class = InsertQuery
            where_cond = None
        else:
            query_class = UpdateQuery
            where_cond = WhereCondition(model.pk_field.query_name, WhereCondition.EQ, self.pk)

        query = query_class(
            table_name=model.table_name, fields=[field.column_name for field in modified_fields], where=where_cond,
        )
        raw_sql = query.render_sql(model.db.spec)
        query_params = [getattr(self, field.name) for field in modified_fields]
        if where_cond:
            query_params.extend(where_cond.values())
        with model.db.cursor() as curr:
            curr.execute(raw_sql, query_params)
        if is_creation:
            setattr(self, model.pk_field.name, curr.lastrowid)

    def _adapt_values(self):
        for field in self.__class__.fields:
            field_name = field.name
            adapted_value = field.adapt_value(getattr(self, field_name))
            setattr(self, field_name, adapted_value)

    def refresh_from_db(self):
        if not self.pk:
            return

        model = self.__class__

        pk_cond = WhereCondition(model.pk_field.query_name, WhereCondition.EQ, self.pk)
        query_fields = [field.query_name for field in model.fields]
        select_query = SelectQuery(table_name=model.table_name, fields=query_fields, where=pk_cond)
        raw_sql = select_query.render_sql(model.db.spec)
        params = pk_cond.values()
        with model.db.cursor() as curr:
            curr.execute(raw_sql, params)
            row = curr.fetchone()
        for i, field in enumerate(model.fields):
            setattr(self, field.name, row[i])

    def delete(self):
        if not self.pk:
            return

        model = self.__class__
        pk_cond = WhereCondition(model.pk_field.query_name, WhereCondition.EQ, self.pk)
        delete_query = DeleteQuery(table_name=model.table_name, where=pk_cond)

        raw_sql = delete_query.render_sql(model.db.spec)
        with model.db.cursor() as curr:
            curr.execute(raw_sql, pk_cond.values())

        setattr(self, model.pk_field.name, None)
        return curr.rowcount
