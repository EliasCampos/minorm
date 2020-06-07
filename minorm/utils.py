from minorm.db import DBDriver


def pk_declaration_for_db(db):
    if db.DRIVER == DBDriver.SQLITE:
        return 'INTEGER PRIMARY KEY AUTOINCREMENT'
    if db.DRIVER == DBDriver.POSTGRES:
        return 'SERIAL PRIMARY KEY'

    raise ValueError('Unsupported DB type.')
