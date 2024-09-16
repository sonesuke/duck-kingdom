use crate::kingdom::connection::connect;
use crate::kingdom::lock::Lock;

mod tests {
    use super::*;
    use crate::kingdom::test_util::TestContext;

    #[test]
    fn basic_without_lock() {
        let _context = TestContext::new("sandbox/test.db");
        let _conn = connect("sandbox/test.db", Lock::NoLock);

        match _conn {
            Ok(_conn) => {
                let result = _conn.sql("SELECT 1;").fetchall();
                assert_eq!(result[0][0], 1);
            }
            Err(_) => assert!(false),
        }
    }

    #[test]
    fn basic_with_overwrite() {
        let _context = TestContext::new("sandbox/test.db");
        let _conn = connect("sandbox/test.db", Lock::Overwrite);

        match _conn {
            Ok(_conn) => {
                let result = _conn.sql("SELECT 1;").fetchall();
                assert_eq!(result[0][0], 1);
            }
            Err(_) => assert!(false),
        }
    }
}
