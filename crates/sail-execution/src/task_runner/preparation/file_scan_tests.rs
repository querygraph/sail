#![expect(clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Result, internal_err};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, ParquetSource};
use datafusion::datasource::source::DataSource;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::Partitioning;
use sail_physical_plan::remote_checkpoint::CheckpointDataSource;

use super::*;
use crate::proto::{decode_remote_physical_plan, encode_remote_physical_plan};

pub(super) fn config(plan: &Arc<dyn ExecutionPlan>) -> &FileScanConfig {
    plan.downcast_ref::<DataSourceExec>()
        .unwrap()
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .unwrap()
}

pub(super) fn assert_groups(expected: &FileScanConfig, actual: &FileScanConfig) {
    assert_eq!(actual.file_groups.len(), expected.file_groups.len());
    for (expected, actual) in expected.file_groups.iter().zip(&actual.file_groups) {
        assert_eq!(actual.len(), expected.len());
        for (expected, actual) in expected.iter().zip(actual.iter()) {
            assert_eq!(actual.object_meta, expected.object_meta);
            assert_eq!(actual.range, expected.range);
            assert_eq!(actual.partition_values, expected.partition_values);
        }
    }
}

#[test]
fn preparation_preserves_decoded_checkpoint_partitioning_and_parquet_adapter() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let source = ParquetSource::new(TableSchema::from(schema));
    let original = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
        .with_file_groups(vec![
            FileGroup::from(vec![PartitionedFile::new("first.parquet", 100)]),
            FileGroup::from(vec![]),
            FileGroup::from(vec![PartitionedFile::new("last.parquet", 200)]),
        ])
        .with_projection_indices(Some(vec![1, 0]))?
        .build();
    let checkpoint = CheckpointDataSource::new(
        Arc::new(original),
        Partitioning::Hash(vec![Arc::new(Column::new("id", 1))], 3),
    );
    let encoded = encode_remote_physical_plan(
        &RemoteExecutionCodec,
        DataSourceExec::from_data_source(checkpoint),
    )?;
    let decoded =
        decode_remote_physical_plan(&TaskContext::default(), &RemoteExecutionCodec, &encoded)?;
    assert!(matches!(
        config(&decoded).output_partitioning(),
        Partitioning::UnknownPartitioning(3)
    ));
    let adapter = config(&decoded).expr_adapter_factory.clone().unwrap();
    let prepared = TaskPreparation::rewrite_file_scans(decoded.clone()).unwrap();
    assert_eq!(prepared.schema(), decoded.schema());
    assert_groups(config(&decoded), config(&prepared));
    assert!(config(&prepared).preserve_order);
    assert!(Arc::ptr_eq(
        &adapter,
        config(&prepared).expr_adapter_factory.as_ref().unwrap()
    ));
    let Partitioning::Hash(expressions, 3) = prepared.output_partitioning() else {
        return internal_err!("preparation discarded the decoded checkpoint partitioning");
    };
    let column = expressions[0].downcast_ref::<Column>().unwrap();
    assert_eq!(column.name(), "id");
    assert_eq!(column.index(), 1);

    // Reapplying preparation must retain an already supplied Parquet adapter.
    let repeated = TaskPreparation::rewrite_file_scans(prepared).unwrap();
    assert!(Arc::ptr_eq(
        &adapter,
        config(&repeated).expr_adapter_factory.as_ref().unwrap()
    ));
    assert!(matches!(
        repeated.output_partitioning(),
        Partitioning::Hash(_, 3)
    ));
    Ok(())
}
