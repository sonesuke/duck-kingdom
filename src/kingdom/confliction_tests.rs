use crate::kingdom::connection::connect;
use crate::kingdom::lock::Lock;

mod tests {
    use super::*;
    use crate::kingdom::test_util::TestContext;

    // Test Confliction under No Lock Mode.
    #[test]
    fn get_overwriting_under_no_lock() {
        let _context = TestContext::new("sandbox/test.db");

        match connect("sandbox/test.db", Lock::NoLock) {
            Ok(_) => match connect("sandbox/test.db", Lock::Overwrite) {
                Ok(_conn_overwrite) => {
                    let _result_overwrite = _conn_overwrite
                        .sql(
                            r#"
                CREATE TABLE test AS SELECT 1;
                SELECT * FROM test;"#,
                        )
                        .fetchall();
                    assert_eq!(_result_overwrite[0][0], 1);
                }
                Err(_) => assert!(false),
            },
            Err(_) => assert!(false),
        }

        // This should be success because the file is locked and we can open it with read-only mode.
        match connect("sandbox/test.db", Lock::NoLock) {
            Ok(_conn) => {
                let _result = _conn.sql("SELECT * FROM test;").fetchall();
                assert_eq!(_result[0][0], 1);
            }
            Err(_) => assert!(false),
        }
    }

    #[test]
    fn get_no_lock_under_no_lock() {
        let _context = TestContext::new("sandbox/test.db");

        match connect("sandbox/test.db", Lock::NoLock) {
            // This should success because the file is not locked and we can open it with read-only mode..
            Ok(_) => match connect("sandbox/test.db", Lock::NoLock) {
                Ok(_conn) => {
                    let _result = _conn.sql("SELECT 1;").fetchall();
                    assert_eq!(_result[0][0], 1);
                }
                Err(_) => assert!(false),
            },
            Err(_) => assert!(false),
        }
    }

    // Test Confliction under Overwrite Mode.
    #[test]
    fn get_overwriting_under_overwriting() {
        let _context = TestContext::new("sandbox/test.db");

        match connect("sandbox/test.db", Lock::Overwrite) {
            Ok(_conn1) => {
                let _result1 = _conn1
                    .sql(
                        r#"
                CREATE TABLE test AS SELECT 3;
                SELECT * FROM test;"#,
                    )
                    .fetchall();
                assert_eq!(_result1[0][0], 3);

                // This should be success because overwriting mode is allowed.
                match connect("sandbox/test.db", Lock::Overwrite) {
                    Ok(_conn2) => {
                        let _result2 = _conn2
                            .sql(
                                r#"
                    CREATE TABLE test AS SELECT 2;
                    SELECT * FROM test;"#,
                            )
                            .fetchall();
                        assert_eq!(_result2[0][0], 2);
                    }
                    Err(_) => assert!(false),
                }
            }
            Err(_) => assert!(false),
        }

        // The connect that closed last should be preffered.
        match connect("sandbox/test.db", Lock::NoLock) {
            Ok(_conn) => {
                let _result = _conn.sql("SELECT * FROM test;").fetchall();
                assert_eq!(_result[0][0], 3);
            }
            Err(_) => assert!(false),
        }
    }

    #[test]
    fn get_no_lock_under_overwriting() {
        let _context = TestContext::new("sandbox/test.db");

        match connect("sandbox/test.db", Lock::Overwrite) {
            // This should success because the file is not locked and we can open it with read-only mode.
            Ok(_) => match connect("sandbox/test.db", Lock::NoLock) {
                Ok(_conn) => {
                    let _result = _conn.sql("SELECT 1;").fetchall();
                    assert_eq!(_result[0][0], 1);
                }
                Err(_) => assert!(false),
            },
            Err(_) => assert!(false),
        }
    }
}
