from collections import namedtuple, OrderedDict

from minorm.db import get_default_db
from minorm.exceptions import DoesNotExists
from minorm.fields import Field, PrimaryKey, ForeignKey
from minorm.managers import QuerySet
from minorm.queries import CreateTableQuery, DropTableQuery, InsertQuery, UpdateQuery
from minorm.utils import pk_declaration_for_db


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

        pk_field = PrimaryKey(column_name=mcs.PK_FIELD, pk_declaration=pk_declaration_for_db(db))
        fields[mcs.PK_FIELD] = pk_field

        # Create and set params:
        model = super().__new__(mcs, name, bases, namespace)
        setattr(model, '_fields', fields)

        pk_field.model = model
        pk_field.name = model.PK_FIELD

        setattr(model, '_meta', model_metadata(db=db, table_name=table_name))

        does_not_exists = type(f'{model.__name__}DoesNotExists', (DoesNotExists, ), {})
        setattr(model, 'DoesNotExists', does_not_exists)

        query_namedtuple = namedtuple(f'{model.__name__}QueryNamedTuple', field_names=model.all_field_names)
        setattr(model, 'query_namedtuple', query_namedtuple)

        return model

    @property
    def objects(cls):
        return QuerySet(model=cls)

    @property
    def fields(cls):
        return OrderedDict((name, field) for name, field in cls._fields.items() if name != cls.PK_FIELD)

    @property
    def all_field_names(cls):
        return [name for name in cls._fields.keys()]

    @property
    def pk_query_name(cls):
        pk_field = cls._fields[cls.PK_FIELD]
        return pk_field.query_name

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
        pk_field = cls.PK_FIELD
        return [pk_field] + [field.column_name for field in cls.fields.values()]

    @property
    def select_field_names(cls):
        return [field.select_field_name for field in cls._fields.values()]

    def instance_from_row(cls, row, related=(), is_tuple=True):
        pk_lookup = f'{cls.name}_{cls.PK_FIELD}'
        pk_value = row[pk_lookup]

        kwargs = {}
        for attr_name, field in cls.fields.items():
            if related and isinstance(field, ForeignKey) and field.to in related:
                value = field.to.instance_from_row(row, related=None, is_tuple=is_tuple)
            else:
                field_lookup = f'{cls.name}_{attr_name}'
                value = row[field_lookup]

            kwargs[attr_name] = value

        if is_tuple:
            kwargs[cls.PK_FIELD] = pk_value
            return cls.query_namedtuple(**kwargs)

        result = cls(**kwargs)
        setattr(result, cls.PK_FIELD, pk_value)
        return result

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

    def check_field(cls, field_name, with_pk=False):
        if field_name not in cls._fields or field_name == cls.PK_FIELD and not with_pk:
            raise ValueError(f'{field_name} is not a valid field for model {cls.__name__}.')

        return cls._fields[field_name]


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
