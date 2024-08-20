import os
import pytest
import duck_kingdom as dk


@pytest.fixture(scope="function", autouse=True)
def scope_function():
    if os.path.exists("sandbox/test.db"):
        os.unlink("sandbox/test.db")
    yield


def test_duck_kingdom():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.LOCK) as conn:
        result = conn.sql("select 1;").fetchall()
        assert result == [(1,)]

    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.OVERWRITE) as conn:
        result = conn.sql("select 1;").fetchall()
        assert result == [(1,)]

    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn:
        result = conn.sql("select 1;").fetchall()
        assert result == [(1,)]


