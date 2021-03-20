from minorm.expressions import WhereCondition
from minorm.fields import Field


class Lookup:
    name = None
    operator = None

    @classmethod
    def matches(cls, lookup_name):
        return lookup_name == cls.name

    def process(self, field_name, value):
        return WhereCondition(field=field_name, op=self.operator, value=value)


@Field.register_lookup
class LessThan(Lookup):
    name = 'lt'
    operator = '<'


@Field.register_lookup
class LessThanOrEqual(Lookup):
    name = 'lte'
    operator = '<='


@Field.register_lookup
class GreaterThan(Lookup):
    name = 'gt'
    operator = '>'


@Field.register_lookup
class GreaterThanOrEqual(Lookup):
    name = 'gte'
    operator = '>='


@Field.register_lookup
class In(Lookup):
    name = 'in'
    operator = 'IN'


@Field.register_lookup
class NotEqual(Lookup):
    name = 'neq'
    operator = '!='


class LikePattern(Lookup):
    operator = 'LIKE'
    like_pattern = ''

    def process(self, field_name, value):
        pattern_value = self.like_pattern.format(value)
        return super().process(field_name, pattern_value)


@Field.register_lookup
class StartsWith(LikePattern):
    name = 'startswith'
    like_pattern = '{0}%'


@Field.register_lookup
class EndsWith(LikePattern):
    name = 'endswith'
    like_pattern = '%{0}'


@Field.register_lookup
class Contains(LikePattern):
    name = 'contains'
    like_pattern = '%{0}%'
