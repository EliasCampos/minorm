

class Field:
    FIELD_TYPE = None

    def __init__(self, null=False, unique=False, default=None, column_name=None, **extra_kwargs):
        self.null = null
        self.unique = unique

        self.default = self.adapt(default)
        self.column_name = column_name

        self.extra_kwargs = extra_kwargs

    def adapt(self, value):
        return value

    def to_sql_value(self, value):
        return str(value)

    def to_sql_declaration(self):
        declaration_parts = [f'{self.column_name} {self.get_field_type()}']

        if not self.null:
            declaration_parts.append('NOT NULL')
        if self.unique:
            declaration_parts.append('UNIQUE')
        if self.default:
            default_value = self.to_sql_value(self.default)
            declaration_parts.append(f'DEFAULT {default_value}')

        return ' '.join(declaration_parts)

    def get_field_type(self):
        return self.FIELD_TYPE

    def __set_name__(self, owner, name):
        if not self.column_name:
            self.column_name = name
