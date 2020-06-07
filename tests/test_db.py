import pytest

from minorm.db import read_connection_string


def test_read_connection_string(mocker):
    mocker.patch('os.getenv', return_value="sqlite://")

    result = read_connection_string()
    assert result == "sqlite://"
