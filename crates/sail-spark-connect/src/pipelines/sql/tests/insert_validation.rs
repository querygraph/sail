use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

#[tokio::test]
async fn insert_parse_error_preempts_catalog_evaluation_in_entire_file()
-> Result<(), Box<dyn std::error::Error>> {
    let called = AtomicBool::new(false);
    let result=prepare(pc::DefineSqlGraphElements{sql_text:Some("SET CATALOG CAST(CAST('bad' AS INT) AS STRING); INSERT INTO missing IF NOT EXISTS SELECT 1".into()),sql_file_path:Some("must-not-appear.sql".into()),..Default::default()},|_| {
        called.store(true,Ordering::SeqCst);
        async {Err(SparkError::internal("catalog resolver must not run"))}
    }).await;
    assert!(!called.load(Ordering::SeqCst));
    let SparkError::Structured(error) = result.err().ok_or("expected parse error")? else {
        return Err("expected raw typed error".into());
    };
    assert_eq!(
        error.class,
        "org.apache.spark.sql.catalyst.parser.ParseException"
    );
    assert_eq!(error.condition, "INVALID_STATEMENT_OR_CLAUSE");
    assert_eq!(error.sql_state, "42601");
    assert_eq!(
        error.parameters,
        HashMap::from([("operation".into(), "INSERT INTO ... IF NOT EXISTS".into())])
    );
    assert!(!error.message.contains("must-not-appear.sql"));
    Ok(())
}
