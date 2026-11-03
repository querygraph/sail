use super::*;

#[test]
fn unsorted_staging_keeps_legacy_heterogeneous_properties() -> Result<()> {
    let registry = SessionRegistry::new(16 << 20);
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::AsStaged);
    for value in [
        Arc::new(StringArray::from(vec!["text"])) as ArrayRef,
        Arc::new(Float64Array::from(vec![7.0])) as ArrayRef,
    ] {
        tx.push_nodes(&RecordBatch::try_from_iter([
            ("id", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
            ("property.value", value),
        ])?)?;
    }
    assert_eq!(tx.finish()?.staged_nodes, 2);
    // Such batches never had a single Arrow schema; the new relational surface
    // does not silently cast them, while staging still accepts them as before.
    assert!(registry.nodes("g").is_err());
    Ok(())
}

fn rows(values: &[(&str, Vec<&str>)]) -> RecordBatch {
    RecordBatch::try_from_iter(values.iter().map(|(name, values)| {
        (
            *name,
            Arc::new(StringArray::from(values.clone())) as ArrayRef,
        )
    }))
    .unwrap()
}

fn stage(registry: &SessionRegistry, nodes: &[&str], edges: &[(&str, &str)]) {
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    tx.push_nodes(&rows(&[("node_id", nodes.to_vec())]))
        .unwrap();
    tx.push_edges(&rows(&[
        ("source", edges.iter().map(|e| e.0).collect()),
        ("target", edges.iter().map(|e| e.1).collect()),
    ]))
    .unwrap();
    tx.finish().unwrap();
}

#[tokio::test]
#[expect(
    clippy::disallowed_methods,
    reason = "standalone DataFusion catalog integration, outside Sail's session catalog"
)]
async fn ordinary_relational_scan_preserves_nodes_edges_and_never_builds_csr() -> Result<()> {
    let registry = SessionRegistry::new(16 << 20);
    stage(&registry, &["a", "b", "isolate"], &[("a", "b"), ("a", "b")]);
    let snapshot = registry.snapshot("g")?;
    assert_eq!(snapshot.name(), "g");
    assert_eq!(snapshot.revision(), 1);
    let ctx = SessionContext::new();
    ctx.register_table("nodes", snapshot.nodes()?)?;
    ctx.register_table("edges", snapshot.edges()?)?;
    let result = ctx.sql("SELECT n.node_id, count(e.target) AS degree FROM nodes n LEFT JOIN edges e ON n.node_id = e.source GROUP BY n.node_id ORDER BY n.node_id")
        .await?.collect().await?;
    let result = arrow::compute::concat_batches(&result[0].schema(), &result)?;
    assert_eq!(
        result.column(0).as_string::<i32>().values().as_slice(),
        b"abisolate"
    );
    let degrees = result
        .column(1)
        .as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(degrees.values().as_ref(), &[2, 0, 0]);
    assert_eq!(registry.list()?[0].projections, 0);
    assert_eq!(snapshot.info()?.projections, 0);
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::disallowed_methods,
    reason = "standalone DataFusion catalog integration, outside Sail's session catalog"
)]
async fn empty_tables_keep_their_normalized_property_schema() -> Result<()> {
    let registry = SessionRegistry::new(16 << 20);
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    tx.push_nodes(&RecordBatch::new_empty(Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("age", DataType::Int16, true),
    ]))))?;
    tx.push_edges(&RecordBatch::new_empty(Arc::new(Schema::new(vec![
        Field::new("source", DataType::Utf8, false),
        Field::new("target", DataType::Utf8, false),
        Field::new("weight", DataType::Float32, true),
    ]))))?;
    tx.finish()?;
    let nodes = registry.nodes("g")?;
    let edges = registry.edges("g")?;
    assert_eq!(
        nodes.schema().field_with_name("property.age")?.data_type(),
        &DataType::Int64
    );
    assert_eq!(
        edges
            .schema()
            .field_with_name("property.weight")?
            .data_type(),
        &DataType::Float64
    );
    let ctx = SessionContext::new();
    ctx.register_table("nodes", nodes)?;
    assert_eq!(
        ctx.table("nodes")
            .await?
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    Ok(())
}

#[test]
fn separately_planned_concurrent_reads_share_one_revision_cache() -> Result<()> {
    prepare_output_schemas()?;
    let registry = SessionRegistry::new(16 << 20);
    stage(&registry, &["a", "b", "c"], &[("a", "b"), ("b", "c")]);
    let mut tables = Vec::new();
    for _ in 0..8 {
        tables.push(registry.algorithm("degree", "g", &Default::default(), ColumnNames::Grust)?);
    }
    let original = registry.store.existing("g")?;
    for table in &tables {
        assert!(Arc::ptr_eq(
            &original,
            &table.session.as_ref().unwrap().store.existing("g")?
        ));
    }
    let barrier = Arc::new(std::sync::Barrier::new(tables.len()));
    let handles: Vec<_> = tables
        .into_iter()
        .map(|table| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                table
                    .batches()
                    .unwrap()
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
            })
        })
        .collect();
    for handle in handles {
        assert_eq!(handle.join().unwrap(), 3);
    }
    assert_eq!(registry.list()?[0].projections, 1);
    let before = registry.memory()?.used_bytes;
    drop(
        registry
            .algorithm("degree", "g", &Default::default(), ColumnNames::Grust)?
            .batches()?,
    );
    assert_eq!(
        registry.memory()?.used_bytes,
        before,
        "second provider reused the cached CSR"
    );
    Ok(())
}

#[tokio::test]
async fn empty_declarations_merge_with_later_heterogeneous_property_batches() -> Result<()> {
    let registry = SessionRegistry::new(16 << 20);
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    tx.push_nodes(&RecordBatch::new_empty(Arc::new(Schema::new(vec![
        Field::new("node_id", DataType::Utf8, false),
        Field::new("age", DataType::Int16, true),
    ]))))?;
    tx.push_nodes(&rows(&[("node_id", vec!["a"])]))?;
    tx.push_nodes(&RecordBatch::try_from_iter([
        (
            "node_id",
            Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
        ),
        ("score", Arc::new(Float64Array::from(vec![7.0])) as ArrayRef),
    ])?)?;
    tx.finish()?;
    let table = registry.nodes("g")?;
    assert_eq!(
        table.schema().field_with_name("property.age")?.data_type(),
        &DataType::Int64
    );
    assert_eq!(
        table
            .schema()
            .field_with_name("property.score")?
            .data_type(),
        &DataType::Float64
    );
    let ctx = SessionContext::new();
    let result = datafusion::physical_plan::collect(
        table.scan(&ctx.state(), None, &[], None).await?,
        ctx.task_ctx(),
    )
    .await?;
    let result = arrow::compute::concat_batches(&table.schema(), &result)?;
    assert_eq!(result.num_rows(), 2);
    assert_eq!(
        result.column_by_name("property.age").unwrap().null_count(),
        2
    );
    assert_eq!(
        result
            .column_by_name("property.score")
            .unwrap()
            .null_count(),
        1
    );
    assert_eq!(
        result
            .column_by_name("present.age")
            .unwrap()
            .as_boolean()
            .true_count(),
        0
    );
    assert_eq!(
        result
            .column_by_name("present.score")
            .unwrap()
            .as_boolean()
            .true_count(),
        1
    );
    assert_eq!(registry.list()?[0].projections, 0);
    Ok(())
}

#[tokio::test]
async fn raw_buffers_pin_old_revision_and_host_lease_after_every_other_owner_drops() -> Result<()> {
    let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new("host lease");
    let weak = Arc::downgrade(&owner);
    let registry = SessionRegistry::new_with_owner(16 << 20, owner);
    stage(&registry, &["a", "b"], &[("a", "b")]);
    let snapshot = registry.snapshot("g")?;
    let table = snapshot.nodes()?;
    stage(&registry, &["x"], &[]);
    assert_eq!(snapshot.revision(), 1);
    assert_eq!(registry.snapshot("g")?.revision(), 2);
    let ctx = SessionContext::new();
    let plan = table.scan(&ctx.state(), None, &[], None).await?;
    let result = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    registry.drop_graph("g")?;
    drop(snapshot);
    drop(table);
    drop(ctx);
    assert!(
        registry.memory()?.used_bytes > 0,
        "consumer buffers retain the old rows' admission"
    );
    drop(registry);
    assert!(
        weak.upgrade().is_some(),
        "exported buffers retain the host lease"
    );
    drop(result);
    assert!(
        weak.upgrade().is_none(),
        "last exported buffer returns the host lease"
    );
    Ok(())
}

#[test]
fn algorithm_results_also_pin_the_host_lease() -> Result<()> {
    prepare_output_schemas()?;
    let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new("host lease");
    let weak = Arc::downgrade(&owner);
    let registry = SessionRegistry::new_with_owner(16 << 20, owner);
    stage(&registry, &["a", "b"], &[("a", "b")]);
    let result = registry
        .algorithm("degree", "g", &Default::default(), ColumnNames::Grust)?
        .batches()?;
    registry.drop_graph("g")?;
    drop(registry);
    assert!(weak.upgrade().is_some());
    drop(result);
    assert!(weak.upgrade().is_none());
    Ok(())
}
