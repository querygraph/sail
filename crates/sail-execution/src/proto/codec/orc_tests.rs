#![expect(clippy::unwrap_used)]

use datafusion::arrow::datatypes::FieldRef;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
use datafusion::physical_plan::ExecutionPlanProperties;
use sail_common::spec;
use sail_data_source::options::r#gen::OrcReadOptions;

use super::*;
use crate::proto::converter::RemotePhysicalProtoConverter;

#[path = "orc_execution_tests.rs"]
mod execution;

fn options() -> OrcReadOptions {
    OrcReadOptions {
        max_encoded_metadata_bytes: 64 * 1024 * 1024,
        max_decoded_metadata_bytes: 64 * 1024 * 1024,
        max_decoded_stripe_footer_bytes: 64 * 1024 * 1024,
        max_schema_depth: 64,
    }
}

fn configured(options: &OrcReadOptions) -> [usize; 4] {
    [
        options.max_encoded_metadata_bytes,
        options.max_decoded_metadata_bytes,
        options.max_decoded_stripe_footer_bytes,
        options.max_schema_depth,
    ]
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

fn decode_node(
    node: &ExtendedPhysicalPlanNode,
    context: &TaskContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    RemoteExecutionCodec.try_decode(
        &node.encode_to_vec(),
        &[],
        context,
        &RemotePhysicalProtoConverter::default(),
    )
}

fn config(plan: &Arc<dyn ExecutionPlan>) -> &FileScanConfig {
    plan.downcast_ref::<DataSourceExec>()
        .unwrap()
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .unwrap()
}

fn orc_node(node: &mut ExtendedPhysicalPlanNode) -> &mut r#gen::OrcExecNode {
    match node.node_kind.as_mut().unwrap() {
        NodeKind::Orc(node) => node,
        _ => unreachable!("test expected an ORC node"),
    }
}

fn time_field(name: &str, precision: i32) -> FieldRef {
    Arc::new(
        Field::new(name, DataType::Time64(TimeUnit::Nanosecond), false).with_metadata(
            HashMap::from([(
                spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
                spec::SparkTimeMetadata::try_new(precision)
                    .unwrap()
                    .to_json()
                    .unwrap(),
            )]),
        ),
    )
}

fn scan(read_options: OrcReadOptions, pool: Arc<dyn MemoryPool>) -> Result<FileScanConfig> {
    let nested = Field::new(
        "nested",
        DataType::Struct(vec![time_field("clock", 1)].into()),
        false,
    );
    let interval = Field::new("span", DataType::Duration(TimeUnit::Microsecond), false)
        .with_metadata(HashMap::from([(
            spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
            spec::SparkIntervalMetadata::try_new(
                spec::IntervalUnit::DayTime,
                Some(spec::IntervalFieldType::Day),
                Some(spec::IntervalFieldType::Second),
            )
            .unwrap()
            .unwrap()
            .to_json()
            .unwrap(),
        )]));
    let file_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            time_field("clock", 3),
            Arc::new(interval),
            Arc::new(nested),
        ],
        HashMap::from([("owner".into(), "retained".into())]),
    ));
    let table = TableSchema::builder(file_schema)
        .with_table_partition_cols(vec![Arc::new(Field::new("part", DataType::Utf8, false))])
        .build();
    let source = OrcSource::try_new(
        table,
        read_options,
        OrcJavaTimeZoneConfig::for_reader_timezone("America/Los_Angeles".into())?,
        "Asia/Kathmandu".into(),
        false,
        pool,
    )?;
    let mut files = vec![];
    for (index, (etag, version)) in [
        (Some("tag-0"), Some("version-0")),
        (Some(""), None),
        (None, Some("")),
    ]
    .into_iter()
    .enumerate()
    {
        let mut file = PartitionedFile::new("same.orc", 100)
            .with_range(index as i64 * 20, (index as i64 + 1) * 20);
        file.object_meta.e_tag = etag.map(str::to_owned);
        file.object_meta.version = version.map(str::to_owned);
        file.partition_values = vec![ScalarValue::Utf8(Some(format!("part-{index}")))];
        files.push(file);
    }
    Ok(
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .with_file_groups(vec![
                FileGroup::from(files[..2].to_vec()),
                FileGroup::from(vec![]),
                FileGroup::from(files[2..].to_vec()),
            ])
            .with_batch_size(Some(2))
            .with_limit(Some(7))
            .build(),
    )
}

fn assert_file_config(expected: &FileScanConfig, actual: &FileScanConfig) {
    assert_eq!(actual.object_store_url, expected.object_store_url);
    assert_eq!(
        actual.file_source.table_schema().table_schema(),
        expected.file_source.table_schema().table_schema()
    );
    assert_eq!(actual.file_groups.len(), expected.file_groups.len());
    for (expected, actual) in expected.file_groups.iter().zip(&actual.file_groups) {
        assert_eq!(actual.len(), expected.len());
        for (expected, actual) in expected.iter().zip(actual.iter()) {
            assert_eq!(actual.object_meta, expected.object_meta);
            assert_eq!(actual.range, expected.range);
            assert_eq!(actual.partition_values, expected.partition_values);
        }
    }
    assert_eq!(actual.batch_size, expected.batch_size);
    assert_eq!(actual.limit, expected.limit);
}

#[test]
fn orc_roundtrip_preserves_full_schema_projection_identity_and_configured_limits() -> Result<()> {
    for values in [
        [0, 1, 2, 0],
        [usize::MAX, usize::MAX, usize::MAX, usize::MAX],
    ] {
        let read_options = OrcReadOptions {
            max_encoded_metadata_bytes: values[0],
            max_decoded_metadata_bytes: values[1],
            max_decoded_stripe_footer_bytes: values[2],
            max_schema_depth: values[3],
        };
        // Driver's tiny finite pool must not leak into the configured wire values.
        let original = scan(read_options, Arc::new(GreedyMemoryPool::new(3)))?;
        for projection in [vec![], vec![4, 1, 1, 3, 2, 0]] {
            let original = FileScanConfigBuilder::from(original.clone())
                .with_projection_indices(Some(projection))?
                .build();
            let plan = DataSourceExec::from_data_source(original.clone()) as Arc<dyn ExecutionPlan>;
            let encoded =
                crate::proto::encode_remote_physical_plan(&RemoteExecutionCodec, plan.clone())?;
            let decoded = crate::proto::decode_remote_physical_plan(
                &TaskContext::default(),
                &RemoteExecutionCodec,
                &encoded,
            )?;
            assert_file_config(&original, config(&decoded));
            assert_eq!(decoded.schema(), plan.schema());
            let source = config(&decoded)
                .file_source
                .downcast_ref::<OrcSource>()
                .unwrap();
            assert_eq!(configured(source.options()), values);
            assert_eq!(
                source.reader_timezone().reader_timezone(),
                "America/Los_Angeles"
            );
            assert_eq!(source.session_timezone(), "Asia/Kathmandu");
            assert!(!source.read_case_sensitive());
            assert!(!source.supports_repartitioning());
            assert_eq!(
                source.reader_timezone(),
                original
                    .file_source
                    .downcast_ref::<OrcSource>()
                    .unwrap()
                    .reader_timezone()
            );
        }
    }
    Ok(())
}

#[test]
fn computed_projection_empty_groups_metadata_wrapper_and_checkpoint_survive() -> Result<()> {
    let original = scan(options(), Arc::new(UnboundedMemoryPool::default()))?;
    let projection = ProjectionExprs::new([
        ProjectionExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("id", 0)),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
            )),
            "computed",
        ),
        ProjectionExpr::new(Arc::new(Column::new("clock", 1)), "aliased_time"),
    ]);
    let projected = FileScanConfigBuilder::from(original.clone())
        .with_source(
            original
                .file_source
                .try_pushdown_projection(&projection)?
                .unwrap(),
        )
        .build();
    let node = encode_node(Arc::new(projected.clone()))?;
    let decoded = decode_node(&node, &TaskContext::default())?;
    assert_eq!(
        decoded.schema(),
        DataSourceExec::from_data_source(projected).schema()
    );

    let empty = FileScanConfigBuilder::from(original.clone())
        .with_file_groups(vec![])
        .build();
    let decoded = decode_node(&encode_node(Arc::new(empty))?, &TaskContext::default())?;
    assert!(config(&decoded).file_groups.is_empty());

    let wrapped = FileScanConfigBuilder::from(original.clone())
        .with_source(Arc::new(InputFileMetadataSource::try_new(
            original.file_source.clone(),
        )?))
        .build();
    let node = encode_node(Arc::new(wrapped.clone()))?;
    assert!(node.file_scan_identity.is_none());
    let decoded = decode_node(&node, &TaskContext::default())?;
    assert_file_config(&wrapped, config(&decoded));
    assert!(config(&decoded).file_source.is::<InputFileMetadataSource>());

    let checkpoint = CheckpointDataSource::new(
        Arc::new(original),
        Partitioning::Hash(vec![Arc::new(Column::new("id", 0))], 3),
    );
    let decoded = decode_node(&encode_node(Arc::new(checkpoint))?, &TaskContext::default())?;
    let Partitioning::Hash(expressions, 3) = decoded.output_partitioning() else {
        return plan_err!("ORC checkpoint partitioning was lost");
    };
    assert_eq!(expressions[0].downcast_ref::<Column>().unwrap().index(), 0);
    Ok(())
}

#[test]
fn missing_options_identity_and_malformed_profile_fail_without_runtime_io() -> Result<()> {
    let original = encode_node(Arc::new(scan(
        options(),
        Arc::new(UnboundedMemoryPool::default()),
    )?))?;
    let context = TaskContext::default(); // No store is registered or accessed.
    let mut node = original.clone();
    node.file_scan_identity = None;
    assert!(
        decode_node(&node, &context)
            .unwrap_err()
            .to_string()
            .contains("requires file identity")
    );
    let mut node = original.clone();
    orc_node(&mut node).options = None;
    assert!(
        decode_node(&node, &context)
            .unwrap_err()
            .to_string()
            .contains("requires read options")
    );
    for index in 0..4 {
        let mut node = original.clone();
        let options = orc_node(&mut node).options.as_mut().unwrap();
        match index {
            0 => options.max_encoded_metadata_bytes = None,
            1 => options.max_decoded_metadata_bytes = None,
            2 => options.max_decoded_stripe_footer_bytes = None,
            3 => options.max_schema_depth = None,
            _ => unreachable!(),
        }
        assert!(
            decode_node(&node, &context)
                .unwrap_err()
                .to_string()
                .contains("Missing ORC read option")
        );
    }
    for json in ["", "{}", "not json"] {
        let mut node = original.clone();
        orc_node(&mut node).reader_timezone_json = json.into();
        assert!(
            decode_node(&node, &context)
                .unwrap_err()
                .to_string()
                .contains("failed to decode ORC reader timezone")
        );
    }
    for (key, value, expected) in [
        (
            "profile_bundle_sha256",
            serde_json::json!("wrong"),
            "profile identity mismatch",
        ),
        (
            "semantics_version",
            serde_json::json!(2),
            "Unsupported ORC Java timezone semantics version",
        ),
        ("unknown", serde_json::json!(true), "unknown field"),
    ] {
        let mut node = original.clone();
        let orc = orc_node(&mut node);
        let mut json: serde_json::Value = serde_json::from_str(&orc.reader_timezone_json).unwrap();
        json[key] = value;
        orc.reader_timezone_json = json.to_string();
        assert!(
            decode_node(&node, &context)
                .unwrap_err()
                .to_string()
                .contains(expected)
        );
    }
    let mut node = original.clone();
    node.file_scan_identity.as_mut().unwrap().groups[0].files[0].location = "wrong.orc".into();
    assert!(
        decode_node(&node, &context)
            .unwrap_err()
            .to_string()
            .contains("identity location")
    );
    let mut node = original;
    node.file_scan_identity.as_mut().unwrap().groups.pop();
    assert!(
        decode_node(&node, &context)
            .unwrap_err()
            .to_string()
            .contains("identity group count")
    );
    Ok(())
}

#[test]
fn outer_compression_is_not_silently_dropped_during_encoding() -> Result<()> {
    let scan = scan(options(), Arc::new(UnboundedMemoryPool::default()))?;
    let scan = FileScanConfigBuilder::from(scan)
        .with_file_compression_type(FileCompressionType::GZIP)
        .build();
    assert!(
        encode_node(Arc::new(scan))
            .unwrap_err()
            .to_string()
            .contains("ORC does not support outer file compression")
    );
    Ok(())
}

#[cfg(target_pointer_width = "32")]
#[test]
fn options_that_exceed_worker_usize_are_rejected() -> Result<()> {
    let mut node = encode_node(Arc::new(scan(
        options(),
        Arc::new(UnboundedMemoryPool::default()),
    )?))?;
    orc_node(&mut node)
        .options
        .as_mut()
        .unwrap()
        .max_schema_depth = Some(u64::MAX);
    assert!(
        decode_node(&node, &TaskContext::default())
            .unwrap_err()
            .to_string()
            .contains("exceeds usize")
    );
    Ok(())
}

#[test]
fn orc_sink_roundtrip_keeps_config_codec_and_schema_admission() -> Result<()> {
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::datasource::physical_plan::FileOutputMode;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::empty::EmptyExec;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("p", DataType::Int64, true),
    ]));
    let config = FileSinkConfig {
        original_url: "file:///t".into(),
        object_store_url: ObjectStoreUrl::parse("file://")?,
        file_group: FileGroup::default(),
        table_paths: vec![ListingTableUrl::parse("file:///t")?],
        output_schema: schema.clone(),
        table_partition_cols: vec![("p".into(), DataType::Null)],
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension: "orc".into(),
        file_output_mode: FileOutputMode::Automatic,
    };
    let compression = OrcWriteCompression::Lz4;
    let sink = OrcSink::try_new(config, OrcWriterOptions { compression })?;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(DataSinkExec::new(
        Arc::new(EmptyExec::new(schema)),
        Arc::new(sink),
        None,
    ));
    let codec = RemoteExecutionCodec;
    let converter = RemotePhysicalProtoConverter::default();
    let mut buffer = vec![];
    codec.try_encode(plan.clone(), &mut buffer, &converter)?;
    let context = TaskContext::default();
    let decoded = codec.try_decode(&buffer, &[], &context, &converter)?;
    let exec = decoded
        .downcast_ref::<DataSinkExec>()
        .ok_or_else(|| plan_datafusion_err!("expected a data sink"))?;
    let decoded = exec
        .sink()
        .downcast_ref::<OrcSink>()
        .ok_or_else(|| plan_datafusion_err!("expected an ORC sink"))?;
    assert_eq!(decoded.options().compression, compression);
    assert_eq!(decoded.config().file_extension, "orc");
    assert_eq!(decoded.config().table_partition_cols.len(), 1);
    Ok(())
}
