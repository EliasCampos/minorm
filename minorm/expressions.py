

class WhereCondition:
    AND = 'AND'
    OR = 'OR'

    NOT = 'NOT'

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
