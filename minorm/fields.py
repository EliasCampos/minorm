from datetime import datetime
from decimal import Decimal


class Field:
    SQL_TYPE = None

    def __init__(self, pk=False, null=False, unique=False, default=None, column_name=None):
        self._pk = pk
        self._null = null
        self._unique = unique
        self._default = default
        self._column_name = column_name
        self._name = None
        self._model = None

    def adapt(self, value):
        return value

    def adapt_value(self, value):
        if value is None and self._null:
            return None

        return self.adapt(value)

    def to_query_parameter(self, value):
        return value

    def render_sql(self):
        column_name = self.column_name
        sql_type = self.render_sql_type()

        constrains = self.get_field_constrains()

        sql_parts = [column_name, sql_type, *constrains]
        return ' '.join(sql_parts)

    def get_field_constrains(self):
        constrains = []
        if self._pk:
            constrains.append('PRIMARY KEY')
        if not self._null:
            constrains.append('NOT NULL')
        if self._unique:
            constrains.append('UNIQUE')
        return constrains

    def render_sql_type(self):
        return self.SQL_TYPE

    def get_default(self):
        if callable(self._default):
            return self._default()
        return self._default

    @property
    def column_name(self):
        return self._column_name

    @property
    def name(self):
        return self._name

    @property
    def model(self):
        return self._model

    def __set_name__(self, owner, name):
        self._model = owner
        self._name = name
        if not self._column_name:
            self._column_name = name

    @property
    def is_pk(self):
        return bool(self._pk)

    @property
    def query_name(self):
        return f'{self.model._meta.table_name}.{self.column_name}'


class IntegerField(Field):
    SQL_TYPE = 'INTEGER'
    adapt = int


class BooleanField(Field):
    SQL_TYPE = 'BOOLEAN'
    adapt = bool


class CharField(Field):

    def __init__(self, pk=False, null=False, unique=False, default=None, column_name=None, **extra_kwargs):
        max_length = extra_kwargs.pop('max_length')
        super().__init__(pk=pk, null=null, unique=unique, default=default, column_name=column_name)
        self.max_length = min(int(max_length), 255)

    def render_sql_type(self):
        return f'VARCHAR({self.max_length})'

    def adapt(self, value):
        if not value:
            return ''
        if isinstance(value, str):
            return value
        if isinstance(value, bytes):
            return value.decode('utf-8')
        return str(value)


class DecimalField(Field):

    def __init__(self, pk=False, null=False, unique=False, default=None, column_name=None, **extra_kwargs):
        max_digits = extra_kwargs.pop('max_digits')
        decimal_places = extra_kwargs.pop('decimal_places')

        super().__init__(pk, null=null, unique=unique, default=default, column_name=column_name)

        self._max_digits = max_digits
        self._decimal_places = decimal_places

    def render_sql_type(self):
        return f'DECIMAL({self._max_digits}, {self._decimal_places})'

    def adapt(self, value):
        if not isinstance(value, Decimal):
            if isinstance(value, float):
                dec = self._decimal_places
                return Decimal(round(value * (10 ** dec))) / Decimal(10 ** dec)
            return Decimal(value)
        return value


class DateTimeField(Field):
    SQL_TYPE = 'DATETIME'
    FORMATS = ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d')

    def adapt(self, value):
        if isinstance(value, str):
            for fmt in self.FORMATS:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    pass
        return value


class AutoField(Field):

    def __init__(self, pk=False, null=False, unique=False, default=None, column_name=None):
        super(AutoField, self).__init__(pk=pk, null=null, unique=unique, default=default, column_name=column_name)
        self._null = self.is_pk
        self._unique = False

    def render_sql_type(self):
        field_type = self.model._meta.db.spec.auto_field_type
        return field_type

    def get_field_constrains(self):
        constrains = super().get_field_constrains()
        constrains.extend(self.model._meta.db.spec.auto_field_constrains)
        return constrains


class ForeignKey(Field):

    def __init__(self, to, pk=False, null=False, unique=False, default=None, column_name=None):
        if not column_name:
            column_name = f"{to._meta.table_name}_id"

        super().__init__(pk=pk, null=null, unique=unique, default=default, column_name=column_name)
        self.to = to

    def adapt(self, value):
        if isinstance(value, self.to):
            return value.pk
        return self.to._meta.pk_field.adapt(value)

    def to_query_parameter(self, value):
        return value.pk if isinstance(value, self.to) else value

    def render_sql_type(self):
        ref_pk_field = self.to._meta.pk_field
        fk_type = 'INTEGER' if isinstance(ref_pk_field, AutoField) else ref_pk_field.render_sql_type()
        return f'{fk_type} REFERENCES {self.to._meta.table_name} ({ref_pk_field.name})'
