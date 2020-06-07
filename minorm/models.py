from collections import namedtuple, OrderedDict

from minorm.db import get_default_db
from minorm.fields import Field, PrimaryKey
from minorm.managers import QueryExpression


model_param_class = namedtuple('Meta', 'db, table_name')


class ModelMeta(type):
    PK_FIELD = 'id'

    def __new__(mcs, name, bases, namespace):
        if not bases:
            return super().__new__(mcs, name, bases, namespace)

        namespace.pop(mcs.PK_FIELD, None)

        # Extract fields:
        fields = OrderedDict()
        for attr_name, class_attr in namespace.items():
            if isinstance(class_attr, Field):
                fields[attr_name] = class_attr

        # Extract meta
        meta = namespace.pop('Meta', None)
        table_name = getattr(meta, 'table_name', name.lower())
        db = getattr(meta, 'db', None) or get_default_db()

        fields[mcs.PK_FIELD] = PrimaryKey(db=db)

        # Create and set params:
        model = super().__new__(mcs, name, bases, namespace)
        setattr(model, '_fields', fields)
        setattr(model, '_meta', model_param_class(db=db, table_name=table_name))
        return model

    @property
    def query(cls):
        return QueryExpression(model=cls)

    @property
    def fields(cls):
        return {name: field for name, field in cls._fields.items() if name != cls.PK_FIELD}

    def check_field(cls, field_name):
        if field_name not in cls.fields and field_name != cls.PK_FIELD:
            raise ValueError(f'{field_name} is not a valid field for model {cls.__name__}.')

    def field_to_sql(cls, field_name, value):
        field = cls.fields[field_name]
        return field.to_sql_value(value)


class Model(metaclass=ModelMeta):

    def __init__(self, **kwargs):
        setattr(self, self.__class__.PK_FIELD, None)
        for field_name, field in self.__class__.fields.items():
            value = kwargs.get(field_name, field.default)
            setattr(self, field_name, value)

    @property
    def pk(self):
        return self.id
