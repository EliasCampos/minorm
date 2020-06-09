from datetime import datetime
from decimal import Decimal


class NoVal:
    pass


class Field:
    FIELD_TYPE = None

    NULL = 'NULL'

    def __init__(self, null=False, unique=False, default=NoVal, column_name=None, **extra_kwargs):
        self._null = null
        self._unique = unique

        self._default = default

        self._extra_kwargs = extra_kwargs

        self.column_name = column_name
        self.name = None
        self.model = None

    def adapt(self, value):
        return value

    def adapt_value(self, value):
        if value is None and self._null:
            return None

        return self.adapt(value)

    def to_query_parameter(self, value):
        return value

    def default_declaration(self):
        return str(self.default) if self._default is not None else self.NULL

    def to_sql_declaration(self):
        declaration_parts = [f'{self.column_name} {self.get_field_type()}']

        if not self._null:
            declaration_parts.append('NOT NULL')
        if self._unique:
            declaration_parts.append('UNIQUE')
        if self._default is not NoVal:
            default_sql = self.default_declaration()
            declaration_parts.append(f'DEFAULT {default_sql}')

        return ' '.join(declaration_parts)

    def get_field_type(self):
        return self.FIELD_TYPE

    @property
    def default(self):
        return self._default if self._default is not NoVal else None

    def __set_name__(self, owner, name):
        self.model = owner
        self.name = name
        if not self.column_name:
            self.column_name = name

    @property
    def query_name(self):
        return f'{self.model.table_name}.{self.column_name}'

    @property
    def select_field_name(self):
        return f'{self.query_name} AS {self.model.name}_{self.name}'


class IntegerField(Field):
    FIELD_TYPE = 'INTEGER'
    adapt = int


class _StringField(Field):

    def adapt(self, value):
        if not value:
            return ''
        if isinstance(value, str):
            return value
        if isinstance(value, bytes):
            return value.decode('utf-8')
        return str(value)


class CharField(_StringField):

    def __init__(self, null=False, unique=False, default=NoVal, column_name=None, **extra_kwargs):
        max_length = extra_kwargs.pop('max_length')
        super().__init__(null=null, unique=unique, default=default, column_name=column_name, **extra_kwargs)
        self.max_length = min(int(max_length), 255)

    def get_field_type(self):
        return f'VARCHAR({self.max_length})'


class DecimalField(_StringField):

    def __init__(self, null=False, unique=False, default=NoVal, column_name=None, **extra_kwargs):
        max_digits = extra_kwargs.pop('max_digits')
        decimal_places = extra_kwargs.pop('decimal_places')

        super().__init__(null=null, unique=unique, default=default, column_name=column_name, **extra_kwargs)

        self.max_digits = max_digits
        self.decimal_places = decimal_places

    def get_field_type(self):
        return f'DECIMAL({self.max_digits}, {self.decimal_places})'

    def adapt(self, value):
        if not isinstance(value, Decimal):
            if isinstance(value, float):
                dec = self.decimal_places
                return Decimal(round(value * (10 ** dec))) / Decimal(10 ** dec)
            return Decimal(value)
        return value


class DateTimeField(Field):
    FIELD_TYPE = 'TIMESTAMP'
    FORMATS = ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d')

    def adapt(self, value):
        if isinstance(value, str):
            for fmt in self.FORMATS:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    pass
        return value


class PrimaryKey(Field):

    def __init__(self, **kwargs):
        kwargs['null'] = True
        kwargs['unique'] = False

        super().__init__(**kwargs)
        self.pk_declaration = kwargs['pk_declaration']

        self.model = kwargs.get('model')
        self.name = kwargs.get('name')

    def get_field_type(self):
        return self.pk_declaration


class ForeignKey(Field):
    CASCADE = 'CASCADE'
    RESTRICT = 'RESTRICT'
    SET_NULL = 'SET NULL'
    SET_DEFAULT = 'SET DEFAULT'

    def __init__(self, null=False, unique=False, default=NoVal, column_name=None, **extra_kwargs):
        ref_model = extra_kwargs.pop('to')
        on_delete = extra_kwargs.pop('on_delete')

        if not column_name:
            column_name = f"{ref_model.table_name}_id"

        super().__init__(null=null, unique=unique, default=default, column_name=column_name, **extra_kwargs)
        self.to = ref_model
        self.on_delete = on_delete

    def adapt(self, value):
        if value is None:
            return None
        if isinstance(value, self.to):
            return value.pk
        return int(value)

    def to_query_parameter(self, value):
        return value.pk if isinstance(value, self.to) else value

    def get_field_type(self):
        return f'INTEGER REFERENCES {self.to.table_name} ({self.to.PK_FIELD})'
