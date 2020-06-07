

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
