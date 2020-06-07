

class NoVal:
    pass


class Field:
    FIELD_TYPE = None

    NULL = 'NULL'

    def __init__(self, null=False, unique=False, default=NoVal, column_name=None, **extra_kwargs):
        self.null = null
        self.unique = unique

        self.default = default
        self.column_name = column_name

        self.extra_kwargs = extra_kwargs

    def adapt(self, value):
        return value

    def to_query_parameter(self, value):
        return value

    def default_declaration(self):
        return str(self.default)

    def to_sql_declaration(self):
        declaration_parts = [f'{self.column_name} {self.get_field_type()}']

        if not self.null:
            declaration_parts.append('NOT NULL')
        if self.unique:
            declaration_parts.append('UNIQUE')
        if self.default is not NoVal:
            default_sql = self.default_declaration() if self.default is not None else self.NULL
            declaration_parts.append(f'DEFAULT {default_sql}')

        return ' '.join(declaration_parts)

    def get_field_type(self):
        return self.FIELD_TYPE

    def __set_name__(self, owner, name):
        if not self.column_name:
            self.column_name = name


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


class PrimaryKey(Field):

    def __init__(self, **kwargs):
        kwargs['null'] = True
        kwargs['unique'] = False

        super().__init__(**kwargs)
        self.pk_declaration = kwargs['pk_declaration']

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
