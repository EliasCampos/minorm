from collections import namedtuple, OrderedDict

from minorm.db import get_default_db
from minorm.exceptions import DoesNotExists
from minorm.fields import Field, PrimaryKey
from minorm.managers import QueryExpression
from minorm.queries import CreateTableQuery, DropTableQuery, InsertQuery, UpdateQuery


model_metadata = namedtuple('Meta', 'db, table_name')


class ModelMetaclass(type):
    PK_FIELD = 'id'

    def __new__(mcs, name, bases, namespace):
        if not bases:
            return super().__new__(mcs, name, bases, namespace)

        # Extract fields:
        fields = OrderedDict()
        for attr_name, class_attr in namespace.items():
            if isinstance(class_attr, Field):
                fields[attr_name] = class_attr

        # Extract meta
        meta = namespace.pop('Meta', None)
        table_name = getattr(meta, 'table_name', name.lower())
        db = getattr(meta, 'db', None) or get_default_db()

        fields[mcs.PK_FIELD] = PrimaryKey(column_name=mcs.PK_FIELD)

        # Create and set params:
        model = super().__new__(mcs, name, bases, namespace)
        setattr(model, '_fields', fields)

        setattr(model, '_meta', model_metadata(db=db, table_name=table_name))

        does_not_exists = type(f'{model.__name__}DoesNotExists', (DoesNotExists, ), {})
        setattr(model, 'DoesNotExists', does_not_exists)

        query_namedtuple = namedtuple(f'{model.__name__}QueryNamedTuple', field_names=model.column_names)
        setattr(model, 'query_namedtuple', query_namedtuple)

        return model

    @property
    def query(cls):
        return QueryExpression(model=cls)

    @property
    def fields(cls):
        return OrderedDict((name, field) for name, field in cls._fields.items() if name != cls.PK_FIELD)

    @property
    def db(cls):
        return cls._meta.db

    @property
    def table_name(cls):
        return cls._meta.table_name

    @property
    def column_names(cls):
        pk_field = cls.PK_FIELD
        return [pk_field] + [field.column_name for field in cls.fields.values()]

    def to_sql(cls):
        field_params = [field.to_sql_declaration() for field in cls._fields.values()]
        create_query = CreateTableQuery(db=cls._meta.db, table_name=cls._meta.table_name, params=field_params)
        return str(create_query)

    def create_table(cls):
        field_params = [field.to_sql_declaration() for field in cls._fields.values()]
        create_query = CreateTableQuery(db=cls._meta.db, table_name=cls._meta.table_name, params=field_params)
        return create_query.execute()

    def drop_table(cls):
        drop_query = DropTableQuery(db=cls._meta.db, table_name=cls._meta.table_name)
        return drop_query.execute()

    def check_field(cls, field_name):
        if field_name not in cls.fields:
            raise ValueError(f'{field_name} is not a valid field for model {cls.__name__}.')


class Model(metaclass=ModelMetaclass):

    def __init__(self, **kwargs):
        setattr(self, self.__class__.PK_FIELD, None)
        for field_name, field in self.__class__.fields.items():
            value = kwargs.get(field_name, field.default)
            setattr(self, field_name, value)

    @property
    def pk(self):
        return getattr(self, self.__class__.PK_FIELD)

    def save(self):
        is_creation = not bool(self.pk)
        self._adapt_values()

        model = self.__class__

        column_names = [field.column_name for field in model.fields.values()]
        values = [getattr(self, field_name) for field_name in model.fields.keys()]

        if is_creation:
            operation_class = InsertQuery
        else:
            operation_class = UpdateQuery

        operation = operation_class(db=model.db, table_name=model.table_name, fields=column_names)
        operation.execute(params=values)

        if is_creation:
            setattr(self, self.__class__.PK_FIELD, model.db.last_insert_row_id())

    def _adapt_values(self):
        for name, field in self.__class__.fields.items():
            adapted_value = field.adapt(getattr(self, name))
            setattr(self, name, adapted_value)
