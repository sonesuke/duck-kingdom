from dataclasses import dataclass
from enum import Enum
import duckdb
from logging import getLogger, DEBUG
from uuid import uuid4
import os
import shutil

logger = getLogger(__name__)
logger.setLevel(DEBUG)


class Lock(Enum):
    NO_LOCK = "NO_LOCK"
    LOCK = "LOCK"
    OVERWRITE = "OVERWRITE"


class LockException(Exception):
    pass


class QueryResult(object):
    def __init__(self, result):
        self.result = result

    def fetchall(self):
        return self.result


@dataclass
class MetaDatabase(object):
    db_file: str
    new_file: str

    def __init__(self, db_file: str):
        self.db_file = db_file
        if not os.path.exists(self.db_file):
            self.brand_new()

    def make_new_file_name(self) -> str:
        uuid = uuid4()
        (file_name, ext) = os.path.splitext(os.path.basename(self.db_file))
        dir_name = os.path.dirname(self.db_file)
        return f"{dir_name}/{file_name}_{uuid}{ext}"

    def new(self) -> str:
        self.new_file = self.make_new_file_name()
        latest_db = self.latest()
        shutil.copyfile(latest_db, self.new_file)
        return self.new_file

    def brand_new(self) -> str:
        self.new_file = self.make_new_file_name()
        with duckdb.connect(self.new_file, read_only=False):
            pass
        self.update(self.new_file)
        return self.new_file

    def update(self, new_file="") -> None:
        new_file = self.new_file if new_file == "" else new_file
        with duckdb.connect(self.db_file, read_only=False) as conn:
            conn.execute(
                f"create table if not exists meta (body text, update_at timestamptz); insert into meta values ('{new_file}', current_timestamp);"
            )

    def latest(self) -> str:
        logger.debug(f"Get latest body file from {self.db_file}")
        with duckdb.connect(self.db_file, read_only=True) as conn:
            result = conn.execute(
                "select body from meta order by update_at desc limit 1;"
            ).fetchall()
            return result[0][0]


class Connection(object):
    db_file: str
    lock: str
    conn: duckdb.DuckDBPyConnection = None
    meta: MetaDatabase = None

    def __init__(self, db_file: str, lock: str):
        self.db_file = db_file
        self.lock = lock
        self.meta = MetaDatabase(db_file=db_file)

    def __enter__(self):
        try:
            logger.debug(f"Opening connection to {self.db_file} with {self.lock}")
            if self.lock == Lock.NO_LOCK:
                latest = self.meta.latest()
                self.conn = duckdb.connect(latest, read_only=True)
            elif self.lock == Lock.OVERWRITE:
                latest = self.meta.new()
                self.conn = duckdb.connect(latest, read_only=False)
            elif self.lock == Lock.LOCK:
                latest = self.meta.new()
                self.conn = duckdb.connect(latest, read_only=False)
        except duckdb.IOException:
            raise LockException(f"Could not open {self.db_file} with {self.lock}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            logger.debug(f"Closing connection to {self.db_file} with {self.lock}")
            if self.lock != Lock.NO_LOCK:
                self.meta.update()
            self.conn.close()

        except duckdb.IOException:
            raise LockException(f"Could not update {self.db_file} with {self.lock}")

    def execute(self, query: str) -> None:
        self.conn.execute(query)

    def sql(self, query: str) -> QueryResult:
        return self.conn.sql(query)


def connect(db_file: str, lock: int) -> Connection:
    return Connection(db_file, lock)
