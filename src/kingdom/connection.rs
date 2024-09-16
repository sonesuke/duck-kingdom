use std::default;

use super::lock::Lock;
use super::meta_database::MetaDatabase;
use duckdb::arrow::record_batch::RecordBatch;

pub struct Connection {
    db_file: String,
    lock: Lock,
    conn: duckdb::Connection,
    meta: MetaDatabase,
}

impl Connection {
    pub fn sql(&self, sql: &str) -> Result<()> {
        let results = self.conn.query_row(sql, [], |row| row.get_ref())?;
        Ok(results)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        match self.lock {
            Lock::Overwrite => {
                self.meta.commit(self.db_file.clone()).unwrap();
            }
            _ => {}
        }
    }
}

pub fn connect(db_file: &str, lock: Lock) -> Result<Connection, String> {
    let meta = MetaDatabase::new(db_file);
    let raw_db_file = match lock {
        Lock::NoLock => meta.latest_db(),
        Lock::Overwrite => meta.new_db(),
    };

    let config = match lock {
        Lock::NoLock => duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly),
        Lock::Overwrite => duckdb::Config::default().access_mode(duckdb::AccessMode::ReadWrite),
    };

    match duckdb::Connection::open_with_flags(raw_db_file.clone(), config.unwrap()) {
        Ok(conn) => {
            return Ok(Connection {
                db_file: raw_db_file,
                lock,
                conn,
                meta,
            });
        }
        Err(_) => {
            return Err("Cannot open the file with read-only mode.".to_string());
        }
    }
}
