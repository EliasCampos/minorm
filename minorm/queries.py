

class DDLQuery:

    def __init__(self, db, table_name, params=None):
        self.db = db
        self.table_name = table_name
        self.params = params or ()

    def execute(self):
        raw_sql = str(self)
        return self.db.execute(raw_sql)


class CreateTableQuery(DDLQuery):
    TEMPLATE = 'CREATE TABLE {table} ({fields});'

    def __str__(self):
        field_part = ', '.join(self.params)
        return self.TEMPLATE.format(table=self.table_name, fields=field_part)


class DropTableQuery(DDLQuery):
    TEMPLATE = 'DROP TABLE {table};'

    def __str__(self):
        return self.TEMPLATE.format(table=self.table_name)


class DMLQuery:
    FETCH = False

    def __init__(self, db, table_name, fields=(), where=None, limit=None):
        self.db = db
        self.table_name = table_name
        self.fields = fields

        self.escape = getattr(db, 'escape', '%s')

        self._where = where
        self._limit = limit

    def execute(self, params=()):
        raw_sql = str(self)
        return self.db.execute(raw_sql, params, fetch=self.FETCH)

    def where(self, expr):
        self._where = expr
        return self

    def limit(self, value):
        self._limit = value
        return self


class InsertQuery(DMLQuery):
    FETCH = False

    def __str__(self):
        fields_part = ', '.join(self.fields)
        values_part = ', '.join(self.escape for _ in self.fields)

        result = f'INSERT INTO {self.table_name} ({fields_part}) VALUES ({values_part});'
        return result

    def execute_many(self, params):
        raw_sql = str(self)
        return self.db.execute(raw_sql, params, many=True)


class UpdateQuery(DMLQuery):
    FETCH = False

    def __str__(self):
        fields_part = ', '.join(f'{field} = {self.escape}' for field in self.fields)

        update_str = f'UPDATE {self.table_name} SET {fields_part}'
        query_parts = [update_str]

        if self._where:
            where_part = f'WHERE {self._where}'
            where_part_proper_escape = where_part.format(escape=self.escape)
            query_parts.append(where_part_proper_escape)

        return f"{' '.join(query_parts)};"


class SelectQuery(DMLQuery):
    FETCH = True

    def __init__(self, db, table_name, fields=(), where=None):
        super().__init__(db, table_name, fields, where)

        self._joins = []
        self._order_by = None

    def __str__(self):
        fields_part = ', '.join(self.fields)

        select_str = f'SELECT {fields_part} FROM {self.table_name}'
        query_parts = [select_str]

        query_parts.extend(str(join) for join in self._joins)

        if self._where:
            where_part = f'WHERE {self._where}'
            where_part_proper_escape = where_part.format(escape=self.escape)
            query_parts.append(where_part_proper_escape)

        if self._order_by:
            order_part = ', '.join(str(ordering) for ordering in self._order_by)
            order_str = f'ORDER BY {order_part}'
            query_parts.append(order_str)

        if self._limit:
            limit_str = f'LIMIT {self._limit}'
            query_parts.append(limit_str)

        return f"{' '.join(query_parts)};"

    def join(self, exprs):
        self._joins.extend(exprs)
        return self

    def order_by(self, exprs):
        self._order_by = exprs
        return self
