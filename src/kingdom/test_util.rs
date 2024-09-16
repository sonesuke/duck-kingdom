use std::fs::remove_file;
use std::path::Path;

pub struct TestContext {
    db_file: String,
}

impl TestContext {
    pub fn new(db_file: &str) -> Self {
        let context = TestContext {
            db_file: db_file.to_string(),
        };
        context.cleanup();
        context
    }

    fn cleanup(&self) {
        let path = Path::new(&self.db_file);
        if path.exists() {
            remove_file(&self.db_file).unwrap();
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.cleanup();
    }
}
