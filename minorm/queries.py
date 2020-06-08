

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

    def __init__(self, db, table_name, fields=(), where=None, order_by=None):
        self.db = db
        self.table_name = table_name
        self.fields = fields
        self.where = where
        self.order_by = order_by

        self.escape = getattr(db, 'escape', '%s')

    def execute(self, params=()):
        raw_sql = str(self)
        return self.db.execute(raw_sql, params, fetch=self.FETCH)


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

        if self.where:
            where_part = f'WHERE {self.where}'
            where_part_proper_escape = where_part.format(escape=self.escape)
            query_parts.append(where_part_proper_escape)

        return f"{' '.join(query_parts)};"


class SelectQuery(DMLQuery):
    FETCH = True

    def __str__(self):
        fields_part = ', '.join(self.fields)

        select_str = f'SELECT {fields_part} FROM {self.table_name}'
        query_parts = [select_str]

        if self.where:
            where_part = f'WHERE {self.where}'
            where_part_proper_escape = where_part.format(escape=self.escape)
            query_parts.append(where_part_proper_escape)

        if self.order_by:
            order_part = ', '.join(str(ordering) for ordering in self.order_by)
            order_str = f'ORDER BY {order_part}'
            query_parts.append(order_str)

        return f"{' '.join(query_parts)};"
