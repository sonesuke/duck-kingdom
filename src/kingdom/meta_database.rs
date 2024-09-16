use std::fs::File;
use std::io::Read;
use std::io::Result;
use std::io::Write;
use std::path::Path;
use uuid;

pub struct MetaDatabase {
    meta_db_file: String,
}

impl MetaDatabase {
    pub fn new(db_file: &str) -> Self {
        MetaDatabase {
            meta_db_file: db_file.to_string(),
        }
    }

    pub fn latest_db(&self) -> String {
        let path = Path::new(&self.meta_db_file);
        if !path.exists() {
            let first_raw_db_file = self.new_db();
            self.prepare_empty_db(&first_raw_db_file).unwrap();
            self.commit(first_raw_db_file.clone()).unwrap();
            first_raw_db_file
        } else {
            self.read_metadata().unwrap()
        }
    }

    pub fn new_db(&self) -> String {
        format!("{}.{}", self.meta_db_file, uuid::Uuid::new_v4())
    }

    pub fn commit(&self, db_file: String) -> Result<()> {
        let mut metadata_file = File::create(self.meta_db_file.clone())?;
        metadata_file.write_all(db_file.as_bytes())?;
        Ok(())
    }

    pub fn read_metadata(&self) -> Result<String> {
        let mut metadata_file = File::open(&self.meta_db_file)?;
        let mut content = String::new();
        metadata_file.read_to_string(&mut content)?;
        Ok(content)
    }

    fn prepare_empty_db(&self, db_file: &str) -> Result<()> {
        let config = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadWrite)
            .unwrap();
        duckdb::Connection::open_with_flags(db_file, config).unwrap();
        Ok(())
    }
}
