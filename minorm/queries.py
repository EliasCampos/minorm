

class DDLQuery:

    def __init__(self, table_name, params=()):
        self.table_name = table_name
        self.params = params

    def render_sql(self):
        raise NotImplementedError


class CreateTableQuery(DDLQuery):
    TEMPLATE = 'CREATE TABLE {table} ({fields});'

    def render_sql(self):
        field_part = ', '.join(self.params)
        return self.TEMPLATE.format(table=self.table_name, fields=field_part)


class DropTableQuery(DDLQuery):
    TEMPLATE = 'DROP TABLE {table};'

    def render_sql(self):
        return self.TEMPLATE.format(table=self.table_name)


class DMLQuery:

    def __init__(self, table_name, fields=(), where=None, limit=None):
        self.table_name = table_name
        self.fields = fields

        self._where = where
        self._limit = limit

    def where(self, expr):
        self._where = expr
        return self

    def limit(self, value):
        self._limit = value
        return self

    def render_sql(self, db_spec):
        raise NotImplementedError


class InsertQuery(DMLQuery):

    def render_sql(self, db_spec):
        fields_part = ', '.join(self.fields)

        value_escape = db_spec.value_escape
        values_part = ', '.join(value_escape for _ in self.fields)

        result = f'INSERT INTO {self.table_name} ({fields_part}) VALUES ({values_part});'
        return result


class UpdateQuery(DMLQuery):

    def render_sql(self, db_spec):
        value_escape = db_spec.value_escape
        fields_part = ', '.join(f'{field} = {value_escape}' for field in self.fields)

        update_str = f'UPDATE {self.table_name} SET {fields_part}'
        query_parts = [update_str]

        if self._where:
            where_part = f'WHERE {self._where}'
            where_part_proper_escape = where_part.format(value_escape)
            query_parts.append(where_part_proper_escape)

        return f"{' '.join(query_parts)};"


class DeleteQuery(DMLQuery):

    def render_sql(self, db_spec):
        update_str = f'DELETE FROM {self.table_name}'
        query_parts = [update_str]

        if self._where:
            value_escape = db_spec.value_escape
            where_part = f'WHERE {self._where}'
            where_part_proper_escape = where_part.format(value_escape)
            query_parts.append(where_part_proper_escape)

        return f"{' '.join(query_parts)};"


class SelectQuery(DMLQuery):

    def __init__(self, table_name, fields=(), where=None):
        super().__init__(table_name, fields, where)

        self._joins = []
        self._order_by = None

    def render_sql(self, db_spec):
        fields_part = ', '.join(self.fields)

        select_str = f'SELECT {fields_part} FROM {self.table_name}'
        query_parts = [select_str]

        query_parts.extend(str(join) for join in self._joins)

        if self._where:
            value_escape = db_spec.value_escape

            where_part = f'WHERE {self._where}'
            where_part_proper_escape = where_part.format(value_escape)
            query_parts.append(where_part_proper_escape)

        if self._order_by:
            order_part = ', '.join(str(ordering) for ordering in self._order_by)
            order_str = f'ORDER BY {order_part}'
            query_parts.append(order_str)

        if self._limit:
            limit_str = f'LIMIT {self._limit}'
            query_parts.append(limit_str)

        return f"{' '.join(query_parts)};"

    def join(self, join_expression):
        self._joins.extend(join_expression)
        return self

    def order_by(self, order_expression):
        self._order_by = order_expression
        return self
