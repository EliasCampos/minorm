import datetime
import decimal


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

    def to_query_parameter(self, value):  # pylint: disable=no-self-use
        """Should take a python value and return data in a format that prepared for use as parameter in query."""
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
    number_type = int

    def to_query_parameter(self, value):
        assert self.number_type, 'number field should declare `number_type` class attribute'
        if value is None:
            return None

        try:
            return self.number_type(value)
        except (TypeError, ValueError) as e:
            raise type(e)(f'Field "{self.name}" expected a number but got {value}.') from e


class FloatField(IntegerField):
    SQL_TYPE = 'REAL'
    number_type = float


class BooleanField(Field):
    SQL_TYPE = 'BOOLEAN'

    def to_query_parameter(self, value):
        if value is None:
            return None
        if value in (True, False):
            return bool(value)  # 1/0 are equal to True/False, bool() converts former to latter

        raise ValueError(f'Field "{self.name}" value must be either True or False.')


class CharField(Field):

    def __init__(self, pk=False, null=False, unique=False, default=None, column_name=None, **extra_kwargs):
        max_length = extra_kwargs.pop('max_length')
        super().__init__(pk=pk, null=null, unique=unique, default=default, column_name=column_name)
        self.max_length = min(int(max_length), 255)

    def render_sql_type(self):
        return f'VARCHAR({self.max_length})'

    def to_query_parameter(self, value):
        if value is None or isinstance(value, str):
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

        self._context = decimal.Context(prec=max_digits)

    def render_sql_type(self):
        return f'DECIMAL({self._max_digits}, {self._decimal_places})'

    def to_query_parameter(self, value):
        if value is None:
            return None
        if isinstance(value, float):
            return self._context.create_decimal_from_float(value)

        try:
            return decimal.Decimal(value)
        except (decimal.InvalidOperation, TypeError, ValueError) as e:
            raise ValueError(f'Field "{self.name}" value must be a decimal number.') from e


class DateField(Field):
    SQL_TYPE = 'DATE'
    FORMAT = '%Y-%m-%d'

    def to_query_parameter(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value.date()
        if isinstance(value, datetime.date):
            return value
        if isinstance(value, str):
            try:
                parsed_result = datetime.datetime.strptime(value, self.FORMAT)
            except ValueError as e:
                raise ValueError(
                    f'"{value}" of field {self.name} has an invalid format.'
                    'It must be in format YYYY-MM-DD.'
                ) from e
            else:
                return parsed_result.date()

        raise TypeError(f'Field "{self.name}" should be of type date or iso-format string but got {value}.')


class DateTimeField(Field):
    SQL_TYPE = 'TIMESTAMP'
    FORMATS = ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d')

    def to_query_parameter(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        if isinstance(value, str):
            for fmt in self.FORMATS:
                try:
                    return datetime.datetime.strptime(value, fmt)
                except ValueError:
                    pass

            raise ValueError(
                f'"{value}" of field {self.name} has an invalid format.'
                'It must be in format YYYY-MM-DD [HH:MM[:ss[.uuuuuu]]].'
            )

        raise TypeError(f'Field "{self.name}" should be of type datetime or iso-format string but got {value}.')


class AutoField(Field):

    def __init__(self, pk=False, null=False, unique=False, default=None, column_name=None):
        super().__init__(pk=pk, null=null, unique=unique, default=default, column_name=column_name)
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
        # pylint: disable=too-many-arguments
        if not column_name:
            column_name = f"{to._meta.table_name}_id"

        super().__init__(pk=pk, null=null, unique=unique, default=default, column_name=column_name)
        self.to = to

    def to_query_parameter(self, value):
        if isinstance(value, self.to):
            return value.pk
        return self.to._meta.pk_field.to_query_parameter(value)

    def render_sql_type(self):
        ref_pk_field = self.to._meta.pk_field
        fk_type = 'INTEGER' if isinstance(ref_pk_field, AutoField) else ref_pk_field.render_sql_type()
        return f'{fk_type} REFERENCES {self.to._meta.table_name} ({ref_pk_field.name})'

    def __set__(self, instance, value):
        if isinstance(value, self.to):
            setattr(instance, self.raw_fk_attr, value.pk)
            setattr(instance, self.cached_instance_attr, value)
        else:
            setattr(instance, self.raw_fk_attr, value)

    def __get__(self, instance, owner):
        if not hasattr(instance, self.cached_instance_attr):
            raw_fk_value = getattr(instance, self.raw_fk_attr)
            if raw_fk_value is None:
                return None

            fetched_instance = self.to.qs.get(pk=raw_fk_value)
            setattr(instance, self.cached_instance_attr, fetched_instance)

        return getattr(instance, self.cached_instance_attr)

    @property
    def raw_fk_attr(self):
        return f'{self.name}_id'

    @property
    def cached_instance_attr(self):
        return f'_{self.name}_cached'
