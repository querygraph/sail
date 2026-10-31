use std::collections::HashMap;

use tonic::Status;
use tonic_types::StatusExt;

use super::*;

fn assert_diagnostic(
    error: SparkError,
    condition: &str,
    sql_state: &str,
    parameters: HashMap<String, String>,
) -> TestResult {
    let SparkError::Structured(error) = error else {
        return Err("materialization cause lost structured Spark diagnostic".into());
    };
    assert_eq!(error.class, "org.apache.spark.sql.AnalysisException");
    assert_eq!(error.condition, condition);
    assert_eq!(error.sql_state, sql_state);
    assert_eq!(error.parameters, parameters);
    if condition == "CANNOT_UPDATE_PARTITION_COLUMNS" {
        assert_eq!(
            error.message,
            format!(
                "Declared partitioning {} conflicts with existing table partitioning {}.\nPlease delete the table or change the declared partitioning to match its partitions.",
                parameters["requestedPartitionColumns"], parameters["existingPartitionColumns"]
            )
        );
    } else {
        assert_eq!(
            error.message,
            "Cannot specify both CLUSTER BY and PARTITIONED BY."
        );
    }

    let status: Status = SparkError::Structured(error).into();
    let details = status.get_error_details();
    let info = details.error_info().ok_or("missing error info")?;
    assert_eq!(info.reason, "org.apache.spark.sql.AnalysisException");
    assert_eq!(info.metadata["errorClass"], condition);
    assert_eq!(info.metadata["sqlState"], sql_state);
    assert_eq!(
        serde_json::from_str::<HashMap<String, String>>(&info.metadata["messageParameters"])?,
        parameters
    );
    Ok(())
}

// MaterializeTablesSuite:467: changed partition rejects and leaves the original
// identity transform intact. The original JVM outer wrapper is not yet ported.
#[tokio::test]
async fn changed_or_omitted_partition_keeps_typed_cause_and_existing_transform() -> TestResult {
    let ctx = context()?;
    let seed = graph("CREATE MATERIALIZED VIEW t8 (x BOOLEAN, y INT) PARTITIONED BY (x) AS SELECT true AS x, 1 AS y").await?;
    materialize(&ctx, &seed).await?;
    for (partition, requested) in [("PARTITIONED BY (y)", "identity(y)"), ("", "")] {
        let next = graph(&format!("CREATE STREAMING TABLE t8 {partition} AS SELECT true AS x, 1 AS y UNION ALL SELECT false AS x, 3 AS y")).await?;
        let error = materialize(&ctx, &next)
            .await
            .err()
            .ok_or("partition change unexpectedly accepted")?;
        assert_diagnostic(
            error,
            "CANNOT_UPDATE_PARTITION_COLUMNS",
            "42000",
            HashMap::from([
                ("existingPartitionColumns".into(), "identity(x)".into()),
                ("requestedPartitionColumns".into(), requested.into()),
            ]),
        )?;
        let status = ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(&["t8"])
            .await?;
        let TableKind::Table {
            columns,
            partition_by,
            ..
        } = status.kind
        else {
            return Err("expected existing table".into());
        };
        assert_eq!(columns.len(), 2);
        assert_eq!(partition_by.len(), 1);
        assert_eq!(partition_by[0].column, "x");
        assert!(matches!(
            partition_by[0].transform,
            None | Some(PartitionTransform::Identity)
        ));
    }
    finish(&ctx).await?;
    Ok(())
}

// MaterializeTablesSuite:1050: partitioning and clustering conflict before any
// target creation, with an AnalysisException cause and exact condition/state.
#[tokio::test]
async fn partition_and_cluster_conflict_keeps_typed_cause_before_creation() -> TestResult {
    let ctx = context()?;
    let graph = graph("CREATE STREAMING TABLE invalid_table PARTITIONED BY (x2) CLUSTER BY (x1, x3) AS SELECT 1 AS x1, 1 AS x2, 'x' AS x3 UNION ALL SELECT 2 AS x1, 3 AS x2, 'y' AS x3").await?;
    let error = materialize(&ctx, &graph)
        .await
        .err()
        .ok_or("conflicting partition and clustering accepted")?;
    assert_diagnostic(
        error,
        "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
        "42908",
        HashMap::new(),
    )?;
    assert!(
        ctx.extension::<CatalogManager>()?
            .get_table_or_view(&["invalid_table"])
            .await
            .is_err()
    );
    finish(&ctx).await?;
    Ok(())
}
