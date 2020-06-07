from collections import namedtuple


class WhereCondition:
    LOOKUPS = (
        ('lt', '<'),
        ('lte', '<='),
        ('gt', '>'),
        ('gte', '>='),
        ('in', 'IN'),
        ('neq', '!='),
    )

    AND = 'AND'
    OR = 'OR'

    NOT = 'NOT'

    EQ = '='

    def __init__(self, field, op, value):
        self.field = field
        self.op = op
        self.value = value

        self._and = None
        self._or = None

        self._negated = False

    def __str__(self):
        result = f'{self.field} {self.op} {self.value}'

        if self._and:
            result = f'{result} {self.AND} {self._and}'

        if self._or:
            result = f'{result} {self.OR} {self._or}'

        if self._negated:
            result = f'{self.NOT} ({result})'

        return result

    def __and__(self, other):
        self._and = other
        return self

    def __or__(self, other):
        self._or = other
        return self

    def __invert__(self):
        self._negated = not self._negated
        return self

    @classmethod
    def resolve_lookup(cls, field_name):
        parts = field_name.split('__')
        field = parts[0]
        if len(parts) == 2:
            lookups = dict(cls.LOOKUPS)
            if parts[1] not in lookups:
                raise ValueError(f'Invalid lookup expression: {parts[1]}')
            lookup = lookups[parts[1]]
        else:
            lookup = cls.EQ

        return field, lookup


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
