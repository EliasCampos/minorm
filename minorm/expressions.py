from collections import namedtuple


LOOKUP_SEPARATOR = '__'


class WhereCondition:
    AND = 'AND'
    OR = 'OR'
    NOT = 'NOT'

    EQ = '='

    IN = 'IN'
    LIKE = 'LIKE'

    LOOKUP_MAPPING = (
        ('lt', '<'),
        ('lte', '<='),
        ('gt', '>'),
        ('gte', '>='),
        ('in', IN),
        ('neq', '!='),
        ('startswith', LIKE),
        ('endswith', LIKE),
        ('contains', LIKE),
    )

    LIKE_PATTERNS = (
        ('startswith', '{0}%'),
        ('endswith', '%{0}'),
        ('contains', '%{0}%')
    )

    MULTIPLE_VALUE_OPS = (IN, )

    def __init__(self, field, op, value, no_escape=False):
        self.field = field
        self.op = op
        self.value = value

        self.no_escape = no_escape

        self._and = None
        self._or = None

        self._negated = False

    def __str__(self):
        result = f'{self.field} {self.op} {self.value if self.no_escape else self.resolved_escape}'

        if self._and:
            result = f'{result} {self.AND} {self._and}'

        if self._or:
            result = f'{result} {self.OR} {self._or}'

        if self._negated:
            result = f'{self.NOT} ({result})'

        return result

    def values(self):
        value = self.value
        result = tuple(value) if self.op in self.MULTIPLE_VALUE_OPS else (value, )

        if self._and:
            result += self._and.values()

        if self._or:
            result += self._or.values()

        return result

    @property
    def resolved_escape(self):
        if self.op in self.MULTIPLE_VALUE_OPS:
            return f"({', '.join('{0}' for _ in range(len(self.value)))})"
        return '{0}'

    def __and__(self, other):
        self._and = other
        return self.clone()

    def __or__(self, other):
        self._or = other
        return self.clone()

    def __invert__(self):
        self._negated = not self._negated
        return self.clone()

    # pylint: disable=protected-access
    def clone(self):
        new_where = self.__class__(field=self.field, op=self.op, value=self.value, no_escape=self.no_escape)
        if self._and:
            new_where._and = self._and.clone()
        if self._or:
            new_where._or = self._or.clone()
        new_where._negated = self._negated
        return new_where


class OrderByExpression(namedtuple('OrderByExpression', 'value, ordering')):
    ASC = 'ASC'
    DESC = 'DESC'

    @classmethod
    def from_field_name(cls, field_name):
        if field_name.startswith('-'):
            value = field_name[1:]
            ordering = cls.DESC
        else:
            value = field_name
            ordering = cls.ASC

        return cls(value=value, ordering=ordering)

    def __str__(self):
        return f'{self.value} {self.ordering}'


class JoinExpression:
    LEFT_OUTER = 'LEFT OUTER'
    INNER = 'INNER'

    def __init__(self, table_name, on, join_type=LEFT_OUTER):
        self.table_name = table_name
        self.on = on
        self.join_type = join_type

    def __str__(self):
        return f'{self.join_type} JOIN {self.table_name} ON {self.on}'

    @classmethod
    def on_pk(cls, outer_table, pk_field, fk_field, join_type=None):
        on = WhereCondition(pk_field, WhereCondition.EQ, fk_field, no_escape=True)
        return cls(table_name=outer_table, on=on, join_type=(join_type or cls.LEFT_OUTER))
