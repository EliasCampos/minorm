from contextlib import contextmanager

from minorm.connectors import connector


def commit(db=None):
    if not db:
        db = connector

    db.connection.commit()


def rollback(db=None):
    if not db:
        db = connector

    db.connection.rollback()


@contextmanager
def atomic(db=None):
    if not db:
        db = connector

    connector.set_autocommit(False)
    with db.connection:
        yield
    connector.set_autocommit(True)
