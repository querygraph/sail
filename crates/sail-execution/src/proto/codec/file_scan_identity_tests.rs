#![expect(clippy::unwrap_used)]

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use datafusion::arrow::array::Int64Array;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::{ExecutionPlanProperties, collect};
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;

use super::*;
use crate::proto::converter::RemotePhysicalProtoConverter;

const FORMATS: [&str; 7] = ["json", "csv", "parquet", "arrow", "avro", "text", "binary"];

fn scan(format: &str) -> Result<FileScanConfig> {
    let schema = TableSchema::builder(Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        true,
    )])))
    .with_table_partition_cols(vec![Arc::new(Field::new("part", DataType::Int32, false))])
    .build();
    let source: Arc<dyn FileSource> = match format {
        "json" => Arc::new(JsonSource::new(schema)),
        "csv" => Arc::new(CsvSource::new(schema)),
        "parquet" => Arc::new(ParquetSource::new(schema)),
        "arrow" => Arc::new(ArrowSource::new_file_source(schema)),
        "avro" => Arc::new(AvroSource::new(schema)),
        "text" => Arc::new(TextSource::new(schema, false, None)),
        "binary" => Arc::new(BinarySource::new(schema)),
        _ => return plan_err!("unknown test format {format}"),
    };
    let mut files = Vec::new();
    for (index, (tag, version)) in [
        (None, Some("")),
        (Some(""), None),
        (Some("\"etag-value\""), Some("version-value")),
        (None, None),
    ]
    .into_iter()
    .enumerate()
    {
        // Duplicate paths with different ranges/identities must remain distinct
        // occurrences. A map keyed only by path would lose this information.
        let mut file = PartitionedFile::new("same.object", 100);
        file.object_meta.last_modified = Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap();
        file.object_meta.e_tag = tag.map(str::to_owned);
        file.object_meta.version = version.map(str::to_owned);
        file.partition_values = vec![ScalarValue::Int32(Some(index as i32))];
        if index < 3 {
            file = file.with_range(index as i64 * 20, index as i64 * 20 + 20);
        }
        files.push(file);
    }
    Ok(
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file_groups(vec![
                FileGroup::from(files[..2].to_vec()),
                FileGroup::from(vec![]),
                FileGroup::from(files[2..].to_vec()),
            ])
            .with_projection_indices(Some(vec![1, 0]))?
            .with_limit(Some(7))
            .build(),
    )
}

fn decoded_config(plan: &Arc<dyn ExecutionPlan>) -> &FileScanConfig {
    plan.downcast_ref::<DataSourceExec>()
        .unwrap()
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .unwrap()
}

fn assert_files(expected: &FileScanConfig, actual: &FileScanConfig, identities: bool) {
    assert_eq!(actual.object_store_url, expected.object_store_url);
    assert_eq!(actual.file_groups.len(), expected.file_groups.len());
    for (left, right) in expected.file_groups.iter().zip(&actual.file_groups) {
        assert_eq!(left.files().len(), right.files().len());
        for (left, right) in left.iter().zip(right.iter()) {
            assert_eq!(right.object_meta.location, left.object_meta.location);
            assert_eq!(right.object_meta.size, left.object_meta.size);
            assert_eq!(
                right.object_meta.last_modified,
                left.object_meta.last_modified
            );
            assert_eq!(right.partition_values, left.partition_values);
            assert_eq!(
                right.range.as_ref().map(|range| (range.start, range.end)),
                left.range.as_ref().map(|range| (range.start, range.end))
            );
            if identities {
                assert_eq!(right.object_meta.e_tag, left.object_meta.e_tag);
                assert_eq!(right.object_meta.version, left.object_meta.version);
            } else {
                assert_eq!(right.object_meta.e_tag, None);
                assert_eq!(right.object_meta.version, None);
            }
        }
    }
}

fn encode_node(source: Arc<dyn DataSource>) -> Result<ExtendedPhysicalPlanNode> {
    let remote = Arc::new(RemoteDataSourceExec::new(&DataSourceExec::new(source)));
    let mut bytes = Vec::new();
    RemoteExecutionCodec.try_encode(
        remote,
        &mut bytes,
        &RemotePhysicalProtoConverter::default(),
    )?;
    Ok(ExtendedPhysicalPlanNode::decode(bytes.as_slice()).unwrap())
}

fn decode_node(node: &ExtendedPhysicalPlanNode) -> Result<Arc<dyn ExecutionPlan>> {
    RemoteExecutionCodec.try_decode(
        &node.encode_to_vec(),
        &[],
        &TaskContext::default(),
        &RemotePhysicalProtoConverter::default(),
    )
}

#[test]
fn all_file_formats_preserve_identity_options_and_file_occurrences() -> Result<()> {
    for format in FORMATS {
        let original = scan(format)?;
        let plan = DataSourceExec::from_data_source(original.clone()) as Arc<dyn ExecutionPlan>;
        let bytes = crate::proto::encode_remote_physical_plan(&RemoteExecutionCodec, plan.clone())?;
        let decoded = crate::proto::decode_remote_physical_plan(
            &TaskContext::default(),
            &RemoteExecutionCodec,
            &bytes,
        )?;
        assert_files(&original, decoded_config(&decoded), true);
        assert_eq!(decoded.schema(), plan.schema(), "{format}");
        assert_eq!(decoded.schema().field(0).name(), "part");
        assert_eq!(decoded.schema().field(1).name(), "value");
        assert_eq!(decoded_config(&decoded).limit, Some(7));
        assert_eq!(decoded.fetch(), plan.fetch());
        assert_eq!(decoded.output_partitioning().partition_count(), 3);
    }
    Ok(())
}

#[test]
fn legacy_absence_and_empty_file_groups_remain_readable() -> Result<()> {
    for format in FORMATS {
        let original = scan(format)?;
        assert!(
            original
                .file_groups
                .iter()
                .flat_map(FileGroup::iter)
                .any(|file| {
                    file.object_meta
                        .e_tag
                        .as_ref()
                        .is_some_and(|tag| !tag.is_empty())
                        && file
                            .object_meta
                            .version
                            .as_ref()
                            .is_some_and(|version| !version.is_empty())
                })
        );
        let mut node = encode_node(Arc::new(original.clone()))?;
        assert!(node.file_scan_identity.is_some());
        node.file_scan_identity = None;
        assert_files(&original, decoded_config(&decode_node(&node)?), false);
        for groups in [vec![], vec![FileGroup::from(vec![])]] {
            let empty = FileScanConfigBuilder::from(original.clone())
                .with_file_groups(groups)
                .build();
            let node = encode_node(Arc::new(empty.clone()))?;
            assert_files(&empty, decoded_config(&decode_node(&node)?), true);
        }
    }
    Ok(())
}

#[test]
fn malformed_sidecar_shape_or_location_is_rejected() -> Result<()> {
    let original = encode_node(Arc::new(scan("json")?))?;
    let identity = original.file_scan_identity.as_ref().unwrap();
    assert_eq!(identity.groups.len(), 3);
    assert_eq!(identity.groups[0].files.len(), 2);
    let mut variants = Vec::new();
    let mut value = identity.clone();
    value.groups.pop();
    variants.push(value);
    let mut value = identity.clone();
    value.groups.push(value.groups[1].clone());
    variants.push(value);
    let mut value = identity.clone();
    value.groups[0].files.pop();
    variants.push(value);
    let mut value = identity.clone();
    let extra = value.groups[0].files[0].clone();
    value.groups[0].files.push(extra);
    variants.push(value);
    let mut value = identity.clone();
    value.groups[0].files[0].location = "different.object".to_string();
    variants.push(value);
    let mut value = identity.clone();
    let unexpected = value.groups[0].files[0].clone();
    value.groups[1].files.push(unexpected);
    variants.push(value);
    for identity in variants {
        let mut node = original.clone();
        node.file_scan_identity = Some(identity);
        assert!(decode_node(&node).is_err());
    }
    Ok(())
}

#[test]
fn input_file_metadata_preserves_inner_identity_and_rejects_outer_sidecars() -> Result<()> {
    let original = scan("json")?;
    let source = Arc::new(InputFileMetadataSource::try_new(
        original.file_source.clone(),
    )?);
    let wrapped = FileScanConfigBuilder::from(original.clone())
        .with_source(source)
        .build();
    let mut node = encode_node(Arc::new(wrapped.clone()))?;
    assert!(matches!(
        node.node_kind,
        Some(NodeKind::InputFileMetadata(_))
    ));
    assert!(node.file_scan_identity.is_none());
    let decoded = decode_node(&node)?;
    let config = decoded_config(&decoded);
    assert_files(&wrapped, config, true);
    assert!(config.file_source.is::<InputFileMetadataSource>());
    assert_eq!(
        decoded.schema(),
        DataSourceExec::from_data_source(wrapped).schema()
    );
    node.file_scan_identity = encode_node(Arc::new(original))?.file_scan_identity;
    assert!(decode_node(&node).is_err());
    Ok(())
}

#[test]
fn non_file_nodes_reject_even_empty_identity_sidecars() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let source = MemorySourceConfig::try_new(&[vec![]], schema, None)?;
    let mut node = encode_node(Arc::new(source))?;
    assert!(node.file_scan_identity.is_none());
    decode_node(&node)?;
    node.file_scan_identity = Some(r#gen::FileScanIdentity { groups: vec![] });
    assert!(decode_node(&node).is_err());
    Ok(())
}

#[test]
fn checkpoint_partitioning_survives_identity_restoration() -> Result<()> {
    let original = scan("parquet")?;
    let key = Arc::new(Column::new("part", 0)) as Arc<dyn PhysicalExpr>;
    let source = Arc::new(CheckpointDataSource::new(
        Arc::new(original.clone()),
        Partitioning::Hash(vec![key], 3),
    ));
    let node = encode_node(source)?;
    let decoded = decode_node(&node)?;
    assert_files(&original, decoded_config(&decoded), true);
    let Partitioning::Hash(expressions, 3) = decoded.output_partitioning() else {
        return plan_err!("checkpoint partitioning lost during identity restoration");
    };
    let column = expressions[0].downcast_ref::<Column>().unwrap();
    assert_eq!(column.name(), "part");
    assert_eq!(column.index(), 0);
    Ok(())
}

#[tokio::test]
async fn decoded_json_scan_executes_from_registered_memory_store() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let path = Path::from("identity/data.json");
    store
        .put(
            &path,
            Bytes::from_static(b"{\"value\":1}\n{\"value\":2}\n").into(),
        )
        .await?;
    let mut meta = store.head(&path).await?;
    // Exercise exact optional-string transport independently of whether this
    // JSON opener implements conditional GET. This test makes no such claim.
    meta.version = Some(String::new());
    let file = PartitionedFile::new_from_meta(meta.clone());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("memory://identity")?,
        Arc::new(JsonSource::new(schema)),
    )
    .with_file_groups(vec![FileGroup::from(vec![file])])
    .build();
    let context = Arc::new(TaskContext::default());
    context
        .runtime_env()
        .register_object_store(&url::Url::parse("memory://identity").unwrap(), store);
    let plan = DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>;
    let bytes = crate::proto::encode_remote_physical_plan(&RemoteExecutionCodec, plan)?;
    let decoded =
        crate::proto::decode_remote_physical_plan(&context, &RemoteExecutionCodec, &bytes)?;
    assert_eq!(
        decoded_config(&decoded).file_groups[0].files()[0].object_meta,
        meta
    );
    let batches = collect(decoded, context).await?;
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, [Some(1), Some(2)]);
    Ok(())
}
