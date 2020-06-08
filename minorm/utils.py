from minorm.db import SQLiteDatabase


def pk_declaration_for_db(db):
    if isinstance(db, SQLiteDatabase):
        return 'INTEGER PRIMARY KEY AUTOINCREMENT'

    raise ValueError('Unsupported DB type.')
