import os
import pytest
import duck_kingdom as dk
import glob


@pytest.fixture(scope="function", autouse=True)
def scope_function():
    if os.path.exists("sandbox/test.db"):
        os.remove("sandbox/test.db")
    yield
    for file in glob.glob("sandbox/test*"):
        os.remove(file)


def test_duck_kingdom_with_no_lock_and_lock():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn1:
        result = conn1.sql("select 1;").fetchall()
        assert result == [(1,)]

        # This should fail because the file is locked and we can't open it with LOCK.

        with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.LOCK) as conn2:
            result = conn2.sql(
                "create table test as select 1; select * from test;"
            ).fetchall()
            assert result == [(1,)]

    # This should work because the file is locked and we can open it with read-only mode.
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn3:
        result = conn3.sql("select * from test;").fetchall()
        assert result == [(1,)]


def test_duck_kingdom_with_no_lock_and_overwrite():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn1:
        result = conn1.sql("select 1;").fetchall()
        assert result == [(1,)]

        # This should fail because the file is locked and we can't open it with OVERWRITE mode.
        with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.OVERWRITE) as conn2:
            result = conn2.sql(
                "create table test as select 1; select * from test;"
            ).fetchall()
            assert result == [(1,)]

    # This should work because the file is locked and we can open it with read-only mode.
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn3:
        result = conn3.sql("select * from test;").fetchall()
        assert result == [(1,)]


def test_duck_kingdom_with_no_lock_and_no_lock():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn1:
        result = conn1.sql("select 1;").fetchall()
        assert result == [(1,)]

        # This should work because the file is locked and we can open it with read-only mode.
        with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn2:
            result = conn2.sql("select 1;").fetchall()
            assert result == [(1,)]


def test_duck_kingdom_with_lock_and_lock():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.LOCK) as conn1:
        result = conn1.sql(
            "create table test as select 1; select * from test;"
        ).fetchall()
        assert result == [(1,)]

        # This should fail because the file is locked and we can't open it with LOCK.
        with pytest.raises(dk.LockException):
            with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.LOCK):
                assert False


def test_duck_kingdom_with_lock_and_overwrite():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.LOCK) as conn1:
        result = conn1.sql(
            "create table test as select 1; select * from test;"
        ).fetchall()
        assert result == [(1,)]

        # This should fail because the file is locked and we can't open it with OVERWRITE mode.
        with pytest.raises(dk.LockException):
            with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.OVERWRITE):
                assert False


def test_duck_kingdom_with_lock_and_no_lock():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.LOCK) as conn1:
        result = conn1.sql(
            "create table test as select 1; select * from test;"
        ).fetchall()
        assert result == [(1,)]

        # This should work because the file is locked and we can open it with read-only mode.
        with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn2:
            result = conn2.sql("select 1;").fetchall()
            assert result == [(1,)]


def test_duck_kingdom_with_overwrite_and_lock():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.OVERWRITE) as conn1:
        result = conn1.sql(
            "create table test as select 1; select * from test;"
        ).fetchall()
        assert result == [(1,)]

        # This should work, but the table test should have been overwritten by conn1.
        with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.LOCK) as conn2:
            result = conn2.sql(
                "create table test as select 2; select * from test;"
            ).fetchall()
            assert result == [(2,)]

    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn3:
        result = conn3.sql("select * from test;").fetchall()
        # The table test should have been overwritten with 1 by last connection close.
        assert result == [(1,)]


def test_duck_kingdom_with_overwrite_and_overwrite():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.OVERWRITE) as conn1:
        result = conn1.sql(
            "create table test as select 1; select * from test;"
        ).fetchall()
        assert result == [(1,)]

        # This should work, but the table test should have been overwritten by conn1.
        with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.OVERWRITE) as conn2:
            result = conn2.sql(
                "create table test as select 2; select * from test;"
            ).fetchall()
            assert result == [(2,)]

    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn3:
        result = conn3.sql("select * from test;").fetchall()
        # The table test should have been overwritten with 1 by last connection close.
        assert result == [(1,)]


def test_duck_kingdom_with_overwrite_and_no_lock():
    with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.OVERWRITE) as conn1:
        result = conn1.sql(
            "create table test as select 1; select * from test;"
        ).fetchall()
        assert result == [(1,)]

        # This should work because the file is locked and we can open it with read-only mode.
        with dk.connect(db_file="sandbox/test.db", lock=dk.Lock.NO_LOCK) as conn2:
            result = conn2.sql("select 1;").fetchall()
            assert result == [(1,)]
