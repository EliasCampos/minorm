import os


DATABASE_URL_PARAM = 'DATABASE_URL'


def read_connection_string():
    return os.getenv(DATABASE_URL_PARAM, '')
