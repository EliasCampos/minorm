import decimal
from typing import Any, Optional, Type, Tuple, Union

from minorm.models import Model

class Field:
    SQL_TYPE: Optional[str] = ...
    _pk: bool = ...
    _null: bool = ...
    _unique: bool = ...
    _default: Optional[Any] = ...
    _column_name: Optional[str] = ...
    _name: Optional[str] = ...
    _model: Optional[Type[Model]] = ...
    def __init__(self, pk: bool = ..., null: bool = ..., unique: bool = ..., default: Optional[Any] = ..., column_name: Optional[str] = ...) -> None: ...
    def to_query_parameter(self, value: Any): ...
    def render_sql(self) -> str: ...
    def get_field_constrains(self) -> Tuple[str]: ...
    def render_sql_type(self) -> str: ...
    def get_default(self) -> Any: ...
    @property
    def column_name(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def model(self) -> Type[Model]: ...
    def __set_name__(self, owner: Type[Model], name: str) -> None: ...
    @property
    def is_pk(self) -> bool: ...
    @property
    def query_name(self) -> str: ...

class IntegerField(Field):
    SQL_TYPE: str = ...
    number_type: Union[Type[int], Type[float]] = ...
    def to_query_parameter(self, value: Any): ...

class FloatField(IntegerField):
    SQL_TYPE: str = ...
    number_type: Type[float] = ...

class BooleanField(Field):
    SQL_TYPE: str = ...
    def to_query_parameter(self, value: Any): ...

class CharField(Field):
    max_length: int = ...
    def __init__(self, max_length: int, pk: bool = ..., null: bool = ..., unique: bool = ..., default: Optional[Any] = ..., column_name: Optional[str] = ...) -> None: ...
    def render_sql_type(self) -> str: ...
    def to_query_parameter(self, value: Any): ...

class DecimalField(Field):
    _max_digits: int = ...
    _decimal_places: int = ...
    _context: decimal.Context = ...
    def __init__(self, max_digits: int, decimal_places: int, pk: bool = ..., null: bool = ..., unique: bool = ..., default: Optional[Any] = ..., column_name: Optional[str] = ...) -> None: ...
    def render_sql_type(self) -> str: ...
    def to_query_parameter(self, value: Any): ...

class DateField(Field):
    SQL_TYPE: str = ...
    FORMAT: str = ...
    def to_query_parameter(self, value: Any): ...

class DateTimeField(Field):
    SQL_TYPE: str = ...
    FORMATS: Tuple[str] = ...
    def to_query_parameter(self, value: Any): ...

class AutoField(Field):
    _null: bool = ...
    _unique: bool = ...
    def __init__(self, pk: bool = ..., null: bool = ..., unique: bool = ..., default: Optional[Any] = ..., column_name: Optional[str] = ...) -> None: ...
    def render_sql_type(self): ...
    def get_field_constrains(self): ...

class ForeignKey(Field):
    to: Type[Model] = ...
    def __init__(self, to: Type[Model], pk: bool = ..., null: bool = ..., unique: bool = ..., default: Optional[Any] = ..., column_name: Optional[str] = ...) -> None: ...
    def to_query_parameter(self, value: Any): ...
    def render_sql_type(self) -> str: ...
    def __set__(self, instance: Model, value: Any) -> None: ...
    def __get__(self, instance: Model, owner: Any) -> Optional[Model]: ...
    @property
    def raw_fk_attr(self) -> str: ...
    @property
    def cached_instance_attr(self) -> str: ...
