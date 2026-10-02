use std::collections::BTreeSet;

use arrow::array::{Float32Array, Float64Array, Int32Array};
use futures::{StreamExt, TryStreamExt};

use super::*;

fn edges(source: &[&str], target: &[&str], weight: Option<&[f64]>) -> RecordBatch {
    let mut fields = vec![
        Field::new("src", DataType::Utf8, false),
        Field::new("dst", DataType::Utf8, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(source.to_vec())),
        Arc::new(StringArray::from(target.to_vec())),
    ];
    if let Some(w) = weight {
        fields.push(Field::new("w", DataType::Float64, false));
        columns.push(Arc::new(Float64Array::from(w.to_vec())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn no_options() -> serde_json::Map<String, serde_json::Value> {
    Default::default()
}

#[test]
fn every_algorithm_grust_registers_is_served_with_its_declared_columns() {
    let names = algorithm_names();
    assert!(names.len() >= 12, "{names:?}");
    for definition in definitions() {
        let name = short(definition);
        let schema = output_schema(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        let produced: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let declared: Vec<&str> = definition.outputs.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(produced, declared, "{name}");
        let produced: Vec<bool> = schema.fields().iter().map(|f| f.is_nullable()).collect();
        let declared: Vec<bool> = definition.outputs.iter().map(|f| f.nullable).collect();
        assert_eq!(produced, declared, "{name}: nullability");
    }
}

/// Whether an Arrow type is, or contains, an unsigned integer.
fn unsigned(data_type: &DataType) -> bool {
    match data_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => unsigned(item.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| unsigned(f.data_type())),
        DataType::Dictionary(key, value) => unsigned(key) || unsigned(value),
        _ => false,
    }
}

/// Spark has no unsigned integer types: the Spark Connect client refuses a
/// `uint64` column outright ("uint64 is not supported in conversion to
/// Arrow"), so one unsigned column makes a whole read fail. No column Nutmeg
/// serves may be unsigned, at any depth: every algorithm's result, observed
/// from a real run, and the graph listing.
#[test]
fn no_column_nutmeg_serves_is_unsigned() {
    let mut schemas = vec![
        ("graphs".to_string(), GraphsTable::arrow_schema()),
        ("memory".to_string(), MemoryTable::arrow_schema()),
    ];
    for name in algorithm_names() {
        let schema = output_schema(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        schemas.push((name.to_string(), schema));
    }
    assert!(algorithm_names().contains(&"pagerank") && algorithm_names().contains(&"degree"));
    for (name, schema) in &schemas {
        for field in schema.fields() {
            assert!(
                !unsigned(field.data_type()),
                "{name}.{} is {}, which Spark cannot represent",
                field.name(),
                field.data_type()
            );
        }
    }
    // The check itself sees an unsigned type, including inside a list.
    assert!(unsigned(&DataType::UInt64));
    assert!(unsigned(&DataType::new_large_list(DataType::UInt64, true)));
    assert!(!unsigned(&DataType::Int64));
}

#[test]
fn names_resolve_in_either_spelling() {
    assert_eq!(resolve_algorithm("shortest_paths"), Some("shortestPaths"));
    assert_eq!(resolve_algorithm("shortestPaths"), Some("shortestPaths"));
    assert_eq!(resolve_algorithm("PAGERANK"), Some("pagerank"));
    // Louvain stood here as the unknown name until Grust registered it.
    assert_eq!(resolve_algorithm("louvain"), Some("louvain"));
    assert_eq!(resolve_algorithm("not_an_algorithm"), None);
    assert_eq!(snake("multiSourceBfs"), "multi_source_bfs");
}

#[test]
fn grust_sail_table_columns_are_recognized_without_a_mapping() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("src_id", DataType::Utf8, false),
            Field::new("dst_id", DataType::Utf8, false),
            Field::new("edge_type", DataType::Utf8, false),
            Field::new("props", DataType::Utf8, false),
            Field::new("hops", DataType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["e1"])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(StringArray::from(vec!["KNOWS"])),
            Arc::new(StringArray::from(vec!["{}"])),
            Arc::new(Int32Array::from(vec![Some(3)])),
        ],
    )
    .unwrap();
    let out = normalize_edges(&batch, &ColumnMapping::default()).unwrap();
    let names: Vec<_> = out
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        [
            "source",
            "target",
            "label",
            "edge_id",
            "property.hops",
            "present.hops"
        ]
    );
    assert_eq!(out.column(4).data_type(), &DataType::Int64);
}

#[test]
fn unknown_options_are_rejected_by_grusts_validator() {
    let mut options = no_options();
    options.insert("dampng".into(), serde_json::json!(0.5));
    let error = validate("pagerank", &options).unwrap_err().to_string();
    assert!(error.contains("dampng"), "{error}");
    let error = validate("bfs", &no_options()).unwrap_err().to_string();
    assert!(error.contains("needs `source`"), "{error}");
}

#[test]
fn string_options_take_the_declared_type_and_spelling() {
    let options = options_from_strings(
        "pagerank",
        [
            ("maxiterations".to_string(), "20".to_string()),
            ("damping".to_string(), "0.9".to_string()),
            ("relationshiptypes".to_string(), "[\"KNOWS\"]".to_string()),
        ],
    )
    .unwrap();
    assert_eq!(options["maxIterations"], serde_json::json!(20));
    assert_eq!(options["damping"], serde_json::json!(0.9));
    assert_eq!(options["relationshipTypes"], serde_json::json!(["KNOWS"]));
    validate("pagerank", &options).unwrap();
    // A numeric-looking node id stays a string.
    let options = options_from_strings("bfs", [("source".to_string(), "42".to_string())]).unwrap();
    assert_eq!(options["source"], serde_json::json!("42"));
}

#[test]
fn projections_are_cached_per_option_set_and_dropped_on_restage() {
    let name = "cache-test";
    let mapping = ColumnMapping::default();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b"], &["b", "c"], None)],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let directed = validate("wcc", &no_options()).unwrap();
    let mut undirected = no_options();
    undirected.insert("orientation".into(), serde_json::json!("undirected"));
    let undirected = validate("wcc", &undirected).unwrap();
    Registry::projection(name, &directed).unwrap();
    Registry::projection(name, &directed).unwrap();
    Registry::projection(name, &undirected).unwrap();
    let info = |n: &str| {
        Registry::list()
            .unwrap()
            .into_iter()
            .find(|g| g.name == n)
            .unwrap()
    };
    assert_eq!(info(name).projections, 2);
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["c"], &["d"], None)],
        &mapping,
        false,
        StageOrder::Canonical,
    )
    .unwrap();
    assert_eq!(info(name).projections, 0);
    assert_eq!(info(name).staged_edges, 3);
    assert!(Registry::drop(name).unwrap());
}

#[test]
fn explicit_nodes_make_unknown_endpoints_an_error() {
    let name = "explicit-nodes";
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .unwrap();
    let mapping = ColumnMapping::default();
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["1"], &["9"], None)],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let error = Registry::projection(name, &validate("degree", &no_options()).unwrap())
        .err()
        .expect("unknown endpoint")
        .to_string();
    assert!(error.contains("missing target"), "{error}");
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["1"], &["2"], None)],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    Registry::projection(name, &validate("degree", &no_options()).unwrap()).unwrap();
}

#[tokio::test]
async fn every_algorithm_runs_through_sql() -> Result<()> {
    Registry::stage(
        "sql",
        Part::Edges,
        &[edges(
            &["a", "b", "c", "a"],
            &["b", "c", "a", "c"],
            Some(&[1.0, 2.0, 3.0, 10.0]),
        )],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )?;
    let ctx = SessionContext::new();
    register(&ctx);
    let count = |sql: &str| {
        let ctx = ctx.clone();
        let sql = sql.to_string();
        async move {
            let batches = ctx.sql(&sql).await?.collect().await?;
            Ok::<usize, DataFusionError>(batches.iter().map(|b| b.num_rows()).sum())
        }
    };
    assert_eq!(count("SELECT * FROM nutmeg_degree('sql')").await?, 3);
    assert_eq!(
        count("SELECT * FROM nutmeg_pagerank('sql', '{\"damping\": 0.9}')").await?,
        3
    );
    assert_eq!(
        count("SELECT * FROM nutmeg_bfs('sql', '{\"source\": \"a\"}')").await?,
        3
    );
    assert_eq!(
        count("SELECT * FROM nutmeg_multi_source_bfs('sql', '{\"sources\": [\"a\", \"b\"]}')")
            .await?,
        3
    );
    assert_eq!(
        count("SELECT * FROM nutmeg_dfs('sql', '{\"source\": \"a\"}')").await?,
        3
    );
    assert_eq!(count("SELECT * FROM nutmeg_wcc('sql')").await?, 3);
    assert_eq!(count("SELECT * FROM nutmeg_scc('sql')").await?, 3);
    assert_eq!(
        count("SELECT * FROM nutmeg_topological_sort('sql')").await?,
        1
    );
    assert_eq!(
        count("SELECT * FROM nutmeg_projection_stats('sql')").await?,
        1
    );
    assert_eq!(count("SELECT * FROM nutmeg_estimate_csr('sql')").await?, 1);
    assert!(count("SELECT * FROM nutmeg_graphs()").await? >= 1);
    assert!(
        count("SELECT * FROM nutmeg_shortest_paths('sql', '{\"source\": \"a\", \"weightProperty\": \"w\"}')").await? >= 2
    );

    // Values, not only shapes: weighted Dijkstra from a; a→c directly costs 10, via b costs 3.
    let batches = ctx
        .sql(
            "SELECT \"nodeId\", distance FROM nutmeg_dijkstra('sql', \
             '{\"source\": \"a\", \"weightProperty\": \"w\"}') ORDER BY \"nodeId\"",
        )
        .await?
        .collect()
        .await?;
    let ids = cast(batches[0].column(0), &DataType::Utf8)?;
    let ids = ids.as_any().downcast_ref::<StringArray>().unwrap();
    let dist = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let got: Vec<(&str, f64)> = (0..ids.len())
        .map(|i| (ids.value(i), dist.value(i)))
        .collect();
    assert_eq!(got, [("a", 0.0), ("b", 1.0), ("c", 3.0)]);

    // PageRank scores form a probability distribution.
    let batches = ctx
        .sql("SELECT score FROM nutmeg_pagerank('sql')")
        .await?
        .collect()
        .await?;
    let scores = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let total: f64 = scores.iter().flatten().sum();
    assert!((total - 1.0).abs() < 1e-6, "{total}");

    // The graph composes with ordinary SQL.
    let joined = count(
        "SELECT d.\"nodeId\" FROM nutmeg_degree('sql') d JOIN nutmeg_wcc('sql') w ON d.\"nodeId\" = w.\"nodeId\"",
    )
    .await?;
    assert_eq!(joined, 3);

    let error = ctx
        .sql("SELECT * FROM nutmeg_degree('nope')")
        .await?
        .collect()
        .await;
    assert!(format!("{:?}", error.err()).contains("no graph named"));
    Ok(())
}

/// Node properties arrive the way a Spark DataFrame hands them over: raw column
/// names, narrower types than a kernel reads, and rows in no particular order.
/// Until staging kept them, every node column but the id and label was dropped,
/// so no kernel that reads node properties could run on a staged graph.
///
/// The check is a value, not a presence. Two triangles joined by one edge,
/// partitioned into the triangles, have modularity 5/14 by hand: m = 7 edges,
/// each triangle has 3 internal edges and degree total 7, so
/// Q = 2 * (3/7 - (7/14)^2) = 5/14. Nodes are staged in reverse, so a row
/// misalignment between the staged columns and the projection would scramble
/// the partition and change Q, not merely fail to find it. Both failures were
/// checked by breaking the fix on purpose: dropping node columns, and
/// misassigning communities to rows, each fail this test.
#[test]
fn staged_node_columns_reach_the_kernels_that_read_them() {
    let name = "node-properties";
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            // Int32 and Float32: narrower than the kernels read, so staging casts.
            Field::new("community", DataType::Int32, false),
            Field::new("lat", DataType::Float32, false),
            Field::new("lon", DataType::Float32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["f", "e", "d", "c", "b", "a"])),
            Arc::new(Int32Array::from(vec![1, 1, 1, 0, 0, 0])),
            // One point for every node: the heuristic is zero, so A* is
            // Dijkstra and its answer is the shortest hop count.
            Arc::new(arrow::array::Float32Array::from(vec![10.0; 6])),
            Arc::new(arrow::array::Float32Array::from(vec![20.0; 6])),
        ],
    )
    .unwrap();
    let mapping = ColumnMapping::default();
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(
            &["a", "b", "c", "c", "d", "e", "f"],
            &["b", "c", "a", "d", "e", "f", "d"],
            None,
        )],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();

    let options: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
        serde_json::json!({ "orientation": "undirected", "communityProperty": "community" }),
    )
    .unwrap();
    let batches = run(
        "modularity",
        name,
        &validate("modularity", &options).unwrap(),
    )
    .unwrap();
    let batch = &batches[0];
    let sizes = batch
        .column_by_name("size")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), 2, "one row per community");
    assert!(sizes.iter().all(|size| size == Some(3)), "{sizes:?}");
    let total = batch
        .column_by_name("totalModularity")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    // A sum of a few terms against a hand-computed quotient: equal to within
    // rounding, not bit for bit.
    assert!((total - 5.0 / 14.0).abs() < 1e-12, "modularity {total}");

    let options: serde_json::Map<String, serde_json::Value> =
        serde_json::from_value(serde_json::json!({
            "source": "a", "target": "f", "orientation": "undirected",
            "latitudeProperty": "lat", "longitudeProperty": "lon",
        }))
        .unwrap();
    let batches = run("astar", name, &validate("astar", &options).unwrap()).unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // a - c - d - f: three hops, four nodes on the path.
    assert_eq!(rows, 4, "{batches:?}");
    let total = batches[0]
        .column_by_name("totalCost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3.0);
    assert!(Registry::drop(name).unwrap());
}

/// The alias table is data about Grust's registry, so it is checked against
/// the registry: every Grust name it renames is one some kernel declares, no
/// rename gives two columns of one kernel the same name, and no kernel claims
/// the option that chooses the names.
#[test]
fn gds_aliases_name_declared_columns_and_never_collide() {
    let declared: HashSet<&str> = definitions()
        .into_iter()
        .flat_map(|d| d.outputs.iter().map(|f| f.name.as_str()))
        .collect();
    for (grust, gds) in GDS_COLUMN_ALIASES {
        assert!(declared.contains(grust), "alias for undeclared `{grust}`");
        assert_ne!(grust, gds);
    }
    for definition in definitions() {
        let name = short(definition);
        let schema = Arc::new(Schema::new(
            definition
                .outputs
                .iter()
                .map(|f| Field::new(f.name.as_str(), DataType::Null, true))
                .collect::<Vec<_>>(),
        ));
        ColumnNames::Gds
            .rename_schema(&schema)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        for field in definition
            .options
            .iter()
            .map(|o| &o.field)
            .chain(definition.arguments.iter().map(|a| &a.field))
        {
            for taken in std::iter::once(COLUMN_NAMES_OPTION).chain(QUERY_OPTIONS) {
                assert!(
                    !field.name.eq_ignore_ascii_case(taken),
                    "{name} declares `{}`, which Nutmeg takes for itself",
                    field.name
                );
            }
        }
    }
}

/// For every kernel and both namings, the schema a table reports is the one
/// its scan returns: `batches` re-checks every batch against it, and fails if
/// a rename were applied to one and not the other.
#[test]
fn every_algorithm_reports_the_columns_its_scan_returns_under_either_naming() {
    // The optimized WCC contract is BIGINT identity, unlike the unrestricted
    // string IDs in the Grust probe. Exercise its real scan on valid IDs too.
    let numeric_store = Arc::new(Store::new(16 << 20));
    let numeric_probe = "numeric-schema-probe";
    numeric_store
        .stage(
            numeric_probe,
            Part::Edges,
            &[edges(&["1", "2"], &["2", "3"], Some(&[1.0, 2.0]))],
            &ColumnMapping::default(),
            true,
            StageOrder::Canonical,
        )
        .unwrap();
    for definition in definitions() {
        let name = short(definition);
        output_schema(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        let (args, store, graph) = if matches!(name, "wccRandomized" | "wccRandomizedFused") {
            (
                validate(name, &Default::default()).unwrap(),
                numeric_store.clone(),
                numeric_probe,
            )
        } else if matches!(name, "bfsDirection" | "ssspDeltaStar") {
            let options = if name == "ssspDeltaStar" {
                serde_json::json!({"source":"1","weightProperty":"w"})
            } else {
                serde_json::json!({"source":"1"})
            };
            (
                validate(name, options.as_object().unwrap()).unwrap(),
                numeric_store.clone(),
                numeric_probe,
            )
        } else {
            let (args, _) =
                probe(name, Default::default()).unwrap_or_else(|e| panic!("{name}: {e}"));
            (args, SCHEMA_STORE.clone(), PROBE)
        };
        let args = Arc::new(args);
        for names in [ColumnNames::Grust, ColumnNames::Gds] {
            let table = AlgorithmTable {
                session: Some(SessionRegistry {
                    store: store.clone(),
                    reads: Default::default(),
                }),
                algorithm: name,
                graph: graph.to_string(),
                args: args.clone(),
                names,
                limits: QueryLimits::default(),
                schema: output_schema_named(name, names).unwrap(),
            };
            let batches = table
                .batches()
                .unwrap_or_else(|e| panic!("{name} {names:?}: {e}"));
            assert!(!batches.is_empty(), "{name} {names:?}");
            let got: Vec<String> = batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let expected: Vec<&str> = definition
                .outputs
                .iter()
                .map(|f| names.rename(&f.name))
                .collect();
            assert_eq!(got, expected, "{name} {names:?}");
        }
    }
}

#[tokio::test]
async fn gds_names_are_chosen_per_read_and_grust_names_stay_reachable() -> Result<()> {
    Registry::stage(
        "gds-names",
        Part::Edges,
        &[edges(&["a", "b", "a"], &["b", "c", "c"], None)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )?;
    let ctx = SessionContext::new();
    register(&ctx);
    let columns = |sql: &str| {
        let ctx = ctx.clone();
        let sql = sql.to_string();
        async move {
            let frame = ctx.sql(&sql).await?;
            let names: Vec<String> = frame
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let rows: usize = frame.collect().await?.iter().map(|b| b.num_rows()).sum();
            Ok::<_, DataFusionError>((names, rows))
        }
    };
    let (grust, rows) = columns(
        "SELECT * FROM nutmeg_yens('gds-names', '{\"source\": \"a\", \"target\": \"c\", \"k\": 2}')",
    )
    .await?;
    assert!(grust.contains(&"pathIndex".to_string()), "{grust:?}");
    assert_eq!(rows, 2);
    let (gds, rows) = columns(
        "SELECT * FROM nutmeg_yens('gds-names', \
         '{\"source\": \"a\", \"target\": \"c\", \"k\": 2, \"columnNames\": \"gds\"}')",
    )
    .await?;
    assert!(gds.contains(&"index".to_string()), "{gds:?}");
    assert!(!gds.contains(&"pathIndex".to_string()), "{gds:?}");
    assert_eq!(rows, 2);
    let (gds, _) = columns(
        "SELECT \"nodeId\", \"ranIterations\", \"didConverge\" \
         FROM nutmeg_pagerank('gds-names', '{\"columnNames\": \"GDS\"}')",
    )
    .await?;
    assert_eq!(gds, ["nodeId", "ranIterations", "didConverge"]);
    let error = match ctx
        .sql("SELECT * FROM nutmeg_pagerank('gds-names', '{\"columnNames\": \"neo4j\"}')")
        .await
    {
        Ok(_) => panic!("an unknown naming was accepted"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("columnNames"), "{error}");
    assert!(Registry::drop("gds-names").unwrap());
    Ok(())
}

/// `linkPrediction` reads a node property for `sameCommunity` alone, so the
/// same kernel runs through both of Nutmeg's paths; `allPairsShortestPaths`
/// streams through its own cursor. Values, on a path a - b - c - d whose
/// communities are {a, b, c} and {d}: the distance-two pairs are (a, c) and
/// (b, d), sharing one neighbour each, in one community and in two.
#[test]
fn link_prediction_and_all_pairs_serve_their_values() {
    let name = "link-prediction";
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("community", DataType::Int32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![0, 0, 0, 1])),
        ],
    )
    .unwrap();
    let mapping = ColumnMapping::default();
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b", "c"], &["b", "c", "d"], None)],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let scores = |metric: &str| {
        let options: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "orientation": "undirected", "metric": metric,
                "communityProperty": "community",
            }))
            .unwrap();
        let args = validate("linkPrediction", &options).unwrap();
        let batches = run("linkPrediction", name, &args).unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let column = |c: &str| cast(batch.column_by_name(c).unwrap(), &DataType::Utf8).unwrap();
            let (first, second) = (column("node1"), column("node2"));
            let first = first.as_any().downcast_ref::<StringArray>().unwrap();
            let second = second.as_any().downcast_ref::<StringArray>().unwrap();
            let score = batch
                .column_by_name("score")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                out.push((
                    first.value(row).to_string(),
                    second.value(row).to_string(),
                    score.value(row),
                ));
            }
        }
        out
    };
    let pair = |a: &str, b: &str, s: f64| (a.to_string(), b.to_string(), s);
    assert_eq!(
        scores("sameCommunity"),
        [pair("a", "c", 1.0), pair("b", "d", 0.0)]
    );
    assert_eq!(
        scores("commonNeighbors"),
        [pair("a", "c", 1.0), pair("b", "d", 1.0)]
    );

    let distances = |options: serde_json::Value| {
        let options: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(options).unwrap();
        let args = validate("allPairsShortestPaths", &options).unwrap();
        let mut out = BTreeMap::new();
        for batch in run("allPairsShortestPaths", name, &args).unwrap() {
            let column = |c: &str| cast(batch.column_by_name(c).unwrap(), &DataType::Utf8).unwrap();
            let (source, target) = (column("sourceNodeId"), column("targetNodeId"));
            let source = source.as_any().downcast_ref::<StringArray>().unwrap();
            let target = target.as_any().downcast_ref::<StringArray>().unwrap();
            let distance = batch
                .column_by_name("distance")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                out.insert(
                    (source.value(row).to_string(), target.value(row).to_string()),
                    distance.value(row),
                );
            }
        }
        out
    };
    let all = distances(serde_json::json!({}));
    let key = |a: &str, b: &str| (a.to_string(), b.to_string());
    assert_eq!(all.get(&key("a", "b")), Some(&1.0), "{all:?}");
    assert_eq!(all.get(&key("a", "c")), Some(&2.0), "{all:?}");
    assert_eq!(all.get(&key("a", "d")), Some(&3.0), "{all:?}");
    assert_eq!(all.get(&key("b", "d")), Some(&2.0), "{all:?}");
    let from_b = distances(serde_json::json!({ "sourceNodes": ["b"] }));
    assert!(!from_b.is_empty());
    assert!(from_b.keys().all(|(source, _)| source == "b"), "{from_b:?}");
    assert_eq!(from_b.get(&key("b", "d")), Some(&2.0), "{from_b:?}");
    assert!(Registry::drop(name).unwrap());
}

/// Every kernel that declares a nullable output is read on a graph where that
/// column holds a null, and the reported schema says nullable for it and for
/// nothing Grust declares non-nullable.
///
/// The schema is observed on a three-node probe on which every node reaches
/// every other, so most nullable columns are full there. Grust's Arrow cursors
/// set a column's nullable flag from whether that batch holds a null, so the
/// probe used to report `distance` as non-nullable and every read with an
/// unreachable node failed its schema check (`dijkstra` in the Citi Bike
/// example). The reverse failed too: `degree` probes unweighted, where
/// `strength` is all null, so a weighted read, where it is full, was refused.
///
/// The graph: a weighted cycle a → b → c → a, and d, staged as a node with no
/// edges. From a, d is unreachable; the cycle leaves `longestPath` without
/// distances; d has no neighbours for a clustering coefficient; d alone in its
/// community has no volume for a conductance; unweighted, `degree` has no
/// strength.
#[test]
fn declared_nullable_outputs_are_nullable_whatever_the_rows_hold() {
    let name = "nullable-outputs";
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("community", DataType::Int32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![0, 0, 0, 1])),
        ],
    )
    .unwrap();
    let mapping = ColumnMapping::default();
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(
            &["a", "b", "c"],
            &["b", "c", "a"],
            Some(&[1.0, 2.0, 3.0]),
        )],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();

    // Kernel, options, the declared-nullable column that holds a null here.
    let cases: Vec<(&str, serde_json::Value, &str)> = vec![
        (
            "bfsDirection",
            serde_json::json!({"source":"a"}),
            "distance",
        ),
        (
            "bfsDirection",
            serde_json::json!({"source":"a"}),
            "parentId",
        ),
        (
            "ssspDeltaStar",
            serde_json::json!({"source":"a","weightProperty":"w"}),
            "distance",
        ),
        ("degree", serde_json::json!({}), "strength"),
        ("bfs", serde_json::json!({ "source": "a" }), "distance"),
        (
            "dijkstra",
            serde_json::json!({ "source": "a", "weightProperty": "w" }),
            "distance",
        ),
        (
            "multiSourceBfs",
            serde_json::json!({ "sources": ["a"] }),
            "distance",
        ),
        ("longestPath", serde_json::json!({}), "distance"),
        (
            "localClusteringCoefficient",
            serde_json::json!({ "orientation": "undirected" }),
            "coefficient",
        ),
        (
            "modularity",
            serde_json::json!({ "orientation": "undirected", "communityProperty": "community" }),
            "conductance",
        ),
        (
            "bellmanFord",
            serde_json::json!({ "source": "a", "weightProperty": "w" }),
            "distance",
        ),
    ];

    // The cases cover exactly the kernels that declare a nullable output, so a
    // new nullable declaration fails here until it is read with a null.
    let declared: BTreeMap<&str, Vec<&str>> = definitions()
        .into_iter()
        .map(|d| {
            let nullable = d
                .outputs
                .iter()
                .filter(|f| f.nullable)
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>();
            (short(d), nullable)
        })
        .filter(|(_, nullable)| !nullable.is_empty())
        .collect();
    let mut covered: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (kernel, _, column) in &cases {
        covered.entry(kernel).or_default().push(column);
    }
    assert_eq!(covered, declared);

    let read = |graph: &str, kernel: &str, options: &serde_json::Value| {
        let options: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(options.clone()).unwrap();
        let table = AlgorithmTable::new(kernel, graph.to_string(), &options)
            .unwrap_or_else(|e| panic!("{kernel}: {e}"));
        let batches = table.batches().unwrap_or_else(|e| panic!("{kernel}: {e}"));
        (table.schema(), batches)
    };
    for (kernel, options, column) in &cases {
        let (schema, batches) = read(name, kernel, options);
        let definition = definition_of(kernel).unwrap();
        for (field, declared) in schema.fields().iter().zip(&definition.outputs) {
            assert_eq!(
                field.is_nullable(),
                declared.nullable,
                "{kernel}.{}",
                field.name()
            );
        }
        let nulls: usize = batches
            .iter()
            .map(|b| b.column_by_name(column).unwrap().null_count())
            .sum();
        assert!(
            nulls > 0,
            "{kernel}: `{column}` holds no null on this graph"
        );
        for batch in &batches {
            assert_eq!(batch.schema().fields(), schema.fields(), "{kernel}");
        }
    }

    // The same columns full, on the cycle alone: the flag must not follow the
    // rows the other way either.
    let full = "nullable-outputs-full";
    Registry::stage(
        full,
        Part::Edges,
        &[edges(
            &["a", "b", "c"],
            &["b", "c", "a"],
            Some(&[1.0, 2.0, 3.0]),
        )],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    for (kernel, options, column) in [
        (
            "degree",
            serde_json::json!({ "weightProperty": "w" }),
            "strength",
        ),
        (
            "dijkstra",
            serde_json::json!({ "source": "a", "weightProperty": "w" }),
            "distance",
        ),
    ] {
        let (schema, batches) = read(full, kernel, &options);
        let nulls: usize = batches
            .iter()
            .map(|b| b.column_by_name(column).unwrap().null_count())
            .sum();
        assert_eq!(nulls, 0, "{kernel}: `{column}` should be full here");
        assert!(
            schema.field_with_name(column).unwrap().is_nullable(),
            "{kernel}"
        );
        for batch in &batches {
            assert!(
                batch
                    .schema()
                    .field_with_name(column)
                    .unwrap()
                    .is_nullable(),
                "{kernel}"
            );
        }
    }
    assert!(Registry::drop(name).unwrap());
    assert!(Registry::drop(full).unwrap());
}

// ------------------------------------------------------------ canonical order

/// A small deterministic generator, so a fixture is the same on every run
/// and needs no dependency.
struct Mix(u64);

impl Mix {
    fn next(&mut self) -> u64 {
        // SplitMix64.
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            items.swap(i, self.below(i + 1));
        }
    }
}

/// Three planted communities of ten nodes, dense inside and sparse between.
/// Ids sort differently as text and as numbers (`"10"` before `"9"`), and
/// include the probe's `a`, `b`, `c`, which every kernel's probe arguments
/// name. Every node carries a column for each property option any registered
/// kernel declares ([`probe_key`]). Edges carry a weight `w`, and every
/// seventh pair is joined a second time with a different weight and no
/// `edge_id`, so the tie-break past `source`, `target` is exercised.
struct Fixture {
    /// (id, community) per node.
    nodes: Vec<(String, i64)>,
    /// (source, target, w) per edge.
    edges: Vec<(String, String, f64)>,
}

fn fixture() -> Fixture {
    let mut mix = Mix(7);
    let mut ids: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
    ids.extend((1..=27).map(|i| i.to_string()));
    let nodes: Vec<(String, i64)> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), (i / 10) as i64))
        .collect();
    let mut edges = Vec::new();
    for (i, (u, cu)) in nodes.iter().enumerate() {
        for (j, (v, cv)) in nodes.iter().enumerate() {
            let chance = if cu == cv { 30 } else { 3 };
            if i != j && mix.below(100) < chance {
                edges.push((u.clone(), v.clone(), (1 + mix.below(5)) as f64));
            }
        }
    }
    let parallel: Vec<_> = edges.iter().step_by(7).cloned().collect();
    for (u, v, w) in parallel {
        edges.push((u, v, w + 0.5));
    }
    Fixture { nodes, edges }
}

/// Split `order` into consecutive runs of the given sizes.
fn runs<'a>(order: &'a [usize], sizes: &[usize]) -> Vec<&'a [usize]> {
    let mut out = Vec::new();
    let mut start = 0;
    for &size in sizes {
        out.push(&order[start..start + size]);
        start += size;
    }
    out
}

impl Fixture {
    /// Node rows in `order`, as batches of the given sizes.
    fn node_batches(&self, order: &[usize], sizes: &[usize]) -> Vec<RecordBatch> {
        runs(order, sizes)
            .into_iter()
            .map(|rows| self.node_batch(rows))
            .collect()
    }

    fn node_batch(&self, rows: &[usize]) -> RecordBatch {
        let community = |i: &usize| self.nodes[*i].1;
        let mut fields = vec![Field::new("id", DataType::Utf8, false)];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            rows.iter()
                .map(|i| self.nodes[*i].0.as_str())
                .collect::<Vec<_>>(),
        ))];
        let mut seen = HashSet::new();
        for name in grust_algorithm_procedures::projection_kernel_names() {
            for declared in grust_algorithm_procedures::node_property_options(name).unwrap() {
                let key = probe_key(declared.option, declared.kind);
                if !seen.insert(key.clone()) {
                    continue;
                }
                let values: ArrayRef =
                    match declared.kind {
                        PropertyKind::Number => Arc::new(Float64Array::from(
                            rows.iter()
                                .map(|i| (*i as f64 * 7.0) % 90.0)
                                .collect::<Vec<_>>(),
                        )),
                        PropertyKind::Integer => Arc::new(Int64Array::from(
                            rows.iter().map(community).collect::<Vec<_>>(),
                        )),
                        PropertyKind::Category => Arc::new(StringArray::from(
                            rows.iter()
                                .map(|i| ["x", "y", "z"][community(i) as usize])
                                .collect::<Vec<_>>(),
                        )),
                        PropertyKind::Vector => Arc::new(
                            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                                rows.iter().map(|i| {
                                    let c = community(i) as f32;
                                    Some(vec![Some(1.0 + c), Some(1.0 + (*i % 3) as f32)])
                                }),
                                2,
                            ),
                        ),
                    };
                fields.push(Field::new(&key, values.data_type().clone(), true));
                columns.push(values);
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    /// Edge rows in `order`, as batches of the given sizes.
    fn edge_batches(&self, order: &[usize], sizes: &[usize]) -> Vec<RecordBatch> {
        runs(order, sizes)
            .into_iter()
            .map(|rows| {
                let source: Vec<&str> = rows.iter().map(|i| self.edges[*i].0.as_str()).collect();
                let target: Vec<&str> = rows.iter().map(|i| self.edges[*i].1.as_str()).collect();
                let w: Vec<f64> = rows.iter().map(|i| self.edges[*i].2).collect();
                edges(&source, &target, Some(&w))
            })
            .collect()
    }
}

/// `n` positions shuffled by `seed`, and batch sizes, also drawn from `seed`,
/// that cover them.
fn arrival(n: usize, seed: u64) -> (Vec<usize>, Vec<usize>) {
    let mut mix = Mix(seed);
    let mut order: Vec<usize> = (0..n).collect();
    mix.shuffle(&mut order);
    let mut sizes = Vec::new();
    let mut left = n;
    while left > 0 {
        let size = (1 + mix.below(n / 3 + 1)).min(left);
        sizes.push(size);
        left -= size;
    }
    (order, sizes)
}

/// Every cell of every result row as text, rows in the order they came: two
/// runs are identical exactly when these are equal. Floats print at full
/// round-trip precision, so a difference in the last bit shows. A failed run
/// is compared by its message.
fn rendered(result: Result<Vec<RecordBatch>>) -> Vec<String> {
    let batches = match result {
        Ok(batches) => batches,
        Err(error) => return vec![format!("error: {error}")],
    };
    let options = arrow::util::display::FormatOptions::default().with_null("null");
    let mut rows = Vec::new();
    for batch in &batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| arrow::util::display::ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = formatters
                .iter()
                .map(|f| f.value(row).to_string())
                .collect();
            rows.push(cells.join(" | "));
        }
    }
    rows
}

/// Every registered kernel with its probe arguments (which name nodes `a`,
/// `b`, `c` and the fixture's property columns), on the outgoing and the
/// undirected projection, unweighted and weighted by `w`. A combination
/// Grust's validator refuses is left out; one that fails when run is kept,
/// and compared by its error.
fn calls() -> Vec<(String, &'static str, ValidatedArguments)> {
    let mut out = Vec::new();
    for algorithm in algorithm_names() {
        for orientation in ["outgoing", "undirected"] {
            for weighted in [false, true] {
                let mut options = serde_json::Map::new();
                options.insert("orientation".into(), serde_json::json!(orientation));
                if weighted {
                    options.insert("weightProperty".into(), serde_json::json!("w"));
                }
                if let Ok(args) = probe_args_with(algorithm, options) {
                    let weight = if weighted { "weighted" } else { "unweighted" };
                    out.push((
                        format!("{algorithm} {orientation} {weight}"),
                        algorithm,
                        args,
                    ));
                }
            }
        }
    }
    out
}

/// Stage the fixture under `name` in the arrival order drawn from `seed`: its
/// nodes (unless `edges_only`) in one write, and its edges in one write per
/// batch when `appends`, the first replacing and the rest appending.
fn stage_fixture(
    name: &str,
    fixture: &Fixture,
    seed: u64,
    edges_only: bool,
    appends: bool,
    order: StageOrder,
) {
    let mapping = ColumnMapping::default();
    if edges_only {
        Registry::stage(name, Part::Nodes, &[], &mapping, true, order).unwrap();
    } else {
        let (rows, sizes) = arrival(fixture.nodes.len(), seed);
        let batches = fixture.node_batches(&rows, &sizes);
        Registry::stage(name, Part::Nodes, &batches, &mapping, true, order).unwrap();
    }
    let (rows, sizes) = arrival(fixture.edges.len(), seed.wrapping_mul(31));
    let batches = fixture.edge_batches(&rows, &sizes);
    if appends {
        for (i, batch) in batches.iter().enumerate() {
            Registry::stage(
                name,
                Part::Edges,
                std::slice::from_ref(batch),
                &mapping,
                i == 0,
                order,
            )
            .unwrap();
        }
    } else {
        Registry::stage(name, Part::Edges, &batches, &mapping, true, order).unwrap();
    }
}

type Calls = [(String, &'static str, ValidatedArguments)];

/// Each call's rendered result on `name`.
fn results(name: &str, calls: &Calls) -> Vec<Vec<String>> {
    calls
        .iter()
        .map(|(_, algorithm, args)| rendered(run(algorithm, name, args)))
        .collect()
}

/// Stage the fixture under a fresh name, collect every call's result, drop it.
fn staged_results(
    calls: &Calls,
    seed: u64,
    edges_only: bool,
    appends: bool,
    order: StageOrder,
) -> Vec<Vec<String>> {
    let name = format!("order-{order:?}-{seed}-{edges_only}-{appends}");
    stage_fixture(&name, &fixture(), seed, edges_only, appends, order);
    let out = results(&name, calls);
    assert!(Registry::drop(&name).unwrap());
    out
}

/// The kernels with a call whose values differ between two stagings, compared
/// as multisets of rows: a different row order alone does not count, only
/// different values.
fn differing(calls: &Calls, left: &[Vec<String>], right: &[Vec<String>]) -> BTreeSet<&'static str> {
    let sorted = |rows: &Vec<String>| {
        let mut rows = rows.clone();
        rows.sort();
        rows
    };
    calls
        .iter()
        .zip(left.iter().zip(right))
        .filter(|(_, (l, r))| sorted(l) != sorted(r))
        .map(|((_, algorithm, _), _)| *algorithm)
        .collect()
}

/// The modularity `algorithm` (Leiden or Louvain) reaches on the fixture staged
/// in the arrival order drawn from `seed`: a measure of the partition found,
/// so a difference in it is a different answer, not a relabelling.
fn modularity_reached(
    algorithm: &str,
    seed: u64,
    edges_only: bool,
    appends: bool,
    order: StageOrder,
) -> f64 {
    let name = format!("modularity-{algorithm}-{order:?}-{seed}-{edges_only}-{appends}");
    stage_fixture(&name, &fixture(), seed, edges_only, appends, order);
    let mut options = serde_json::Map::new();
    options.insert("orientation".into(), serde_json::json!("undirected"));
    let batches = run(algorithm, &name, &validate(algorithm, &options).unwrap()).unwrap();
    assert!(Registry::drop(&name).unwrap());
    batches[0]
        .column_by_name("modularity")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

/// The same graph staged in different orders gives identical results, row for
/// row and bit for bit, for every kernel Grust registers, under canonical
/// order; and as staged, it does not.
///
/// Each staging shuffles the fixture's node and edge rows and splits them
/// into batches of random sizes. Every kernel runs with its probe arguments,
/// outgoing and undirected, unweighted and weighted: 164 calls on the Grust
/// this was written against, each compared in full, including the edge
/// ordinals path kernels report and the text of any error. It runs twice:
/// with the nodes staged, and with edges alone, where the nodes are derived
/// from the edges, as the Citi Bike example stages its trips.
///
/// The second half is what shows the sort is doing the work. Staged as they
/// arrived, the same three orders change Leiden's, Louvain's and label
/// propagation's answers, and Leiden and Louvain reach a different modularity
/// in four orders (0.5026 in some, 0.5248 in others): a different partition,
/// not the same one relabelled. Canonically staged, the same four orders all
/// reach one. Without the sort, the first half fails the same way.
#[test]
fn canonical_order_makes_every_kernel_independent_of_arrival_order() {
    let calls = calls();
    assert!(calls.len() >= 100, "{} calls", calls.len());
    for edges_only in [false, true] {
        let canonical: Vec<_> = [1, 2, 3]
            .map(|seed| staged_results(&calls, seed, edges_only, false, StageOrder::Canonical))
            .into();
        let succeeded = canonical[0]
            .iter()
            .filter(|rows| !rows.iter().any(|row| row.starts_with("error:")))
            .count();
        assert!(
            succeeded * 2 > calls.len(),
            "only {succeeded} of {} calls ran; the comparison would be of errors",
            calls.len()
        );
        for (i, (label, _, _)) in calls.iter().enumerate() {
            for other in &canonical[1..] {
                assert_eq!(
                    canonical[0][i], other[i],
                    "{label} (edges only: {edges_only})"
                );
            }
        }

        let as_staged: Vec<_> = [1, 2, 3]
            .map(|seed| staged_results(&calls, seed, edges_only, false, StageOrder::AsStaged))
            .into();
        let mut sensitive = BTreeSet::new();
        for other in &as_staged[1..] {
            sensitive.extend(differing(&calls, &as_staged[0], other));
        }
        for kernel in ["leiden", "louvain", "labelPropagation"] {
            assert!(
                sensitive.contains(kernel),
                "{kernel} gave the same values in every arrival order as staged, so this \
                 fixture does not show what the sort is for (edges only: {edges_only}); \
                 sensitive: {sensitive:?}"
            );
        }
    }
    for algorithm in ["leiden", "louvain"] {
        for edges_only in [false, true] {
            let reached = |order| -> BTreeSet<u64> {
                [1, 2, 3, 4]
                    .map(|seed| {
                        modularity_reached(algorithm, seed, edges_only, false, order).to_bits()
                    })
                    .into()
            };
            let context = format!("{algorithm} (edges only: {edges_only})");
            assert_eq!(reached(StageOrder::Canonical).len(), 1, "{context}");
            assert!(reached(StageOrder::AsStaged).len() > 1, "{context}");
        }
    }
}

/// Canonical order covers the whole part, not each write: the edges staged in
/// one write, or appended a batch at a time in two different orders, give the
/// same results for every call. As staged, the appends give different
/// answers. A canonical append after an as-staged write sorts the rows that
/// write left too.
#[test]
fn canonical_order_covers_the_whole_part_across_appends() {
    let calls = calls();
    let whole = staged_results(&calls, 1, true, false, StageOrder::Canonical);
    for seed in [4, 5] {
        let appended = staged_results(&calls, seed, true, true, StageOrder::Canonical);
        for (i, (label, _, _)) in calls.iter().enumerate() {
            assert_eq!(whole[i], appended[i], "{label} (appends from seed {seed})");
        }
    }
    let left = staged_results(&calls, 4, true, true, StageOrder::AsStaged);
    let right = staged_results(&calls, 5, true, true, StageOrder::AsStaged);
    let sensitive = differing(&calls, &left, &right);
    assert!(
        sensitive.contains("leiden") && sensitive.contains("louvain"),
        "{sensitive:?}"
    );
    let reached: BTreeSet<u64> = [4, 5, 6]
        .map(|seed| modularity_reached("leiden", seed, true, true, StageOrder::AsStaged).to_bits())
        .into();
    assert!(
        reached.len() > 1,
        "as staged, appends reached one modularity"
    );

    // An as-staged write, then a canonical append: the whole part is sorted.
    let name = "order-mixed";
    let fixture = fixture();
    let mapping = ColumnMapping::default();
    let (rows, sizes) = arrival(fixture.edges.len(), 9);
    let batches = fixture.edge_batches(&rows, &sizes);
    let (first, rest) = batches.split_at(batches.len() / 2);
    Registry::stage(name, Part::Nodes, &[], &mapping, true, StageOrder::AsStaged).unwrap();
    Registry::stage(
        name,
        Part::Edges,
        first,
        &mapping,
        true,
        StageOrder::AsStaged,
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        rest,
        &mapping,
        false,
        StageOrder::Canonical,
    )
    .unwrap();
    let mixed = results(name, &calls);
    assert!(Registry::drop(name).unwrap());
    for (i, (label, _, _)) in calls.iter().enumerate() {
        assert_eq!(
            whole[i], mixed[i],
            "{label} (as staged, then a canonical append)"
        );
    }
}

/// `ids` = `int64` keeps integer ids as Int64 through staging, so canonical
/// order is numeric, derived nodes are Int64, and the projection takes Grust's
/// integer path. Kernels answer what they answer for the same graph staged as
/// text, because a node's external id is its decimal text either way.
#[test]
fn integer_identity_is_opt_in_and_answers_what_text_identity_answers() {
    let mut integer = ColumnMapping::default();
    assert!(integer.set("ids", "int64".into()));
    assert!(integer.integer_ids);
    assert!(!integer.clone().set("ids", "float".into()));
    let text = ColumnMapping::default();

    let node_ids = [9, 10, 2, -3, 40];
    let (sources, targets) = ([9, 9, 10, 9, -3], [2, 2, 9, 10, 9]);
    let weights = [3.0, 1.0, 5.0, 2.0, 4.0];
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(node_ids.to_vec()))],
    )
    .unwrap();
    let links = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int32, false),
            Field::new("w", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(sources.to_vec())),
            Arc::new(Int32Array::from(targets.to_vec())),
            Arc::new(Float64Array::from(weights.to_vec())),
        ],
    )
    .unwrap();
    let integers_of = |batches: &[RecordBatch], column: &str| -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                let c = b
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("an Int64 column")
                    .clone();
                (0..c.len()).map(move |i| c.value(i))
            })
            .collect()
    };

    // Canonical order of integer ids is numeric: 2 before 9 before 10.
    let name = "integer-identity";
    for (part, batch) in [(Part::Nodes, &nodes), (Part::Edges, &links)] {
        Registry::stage(
            name,
            part,
            std::slice::from_ref(batch),
            &integer,
            true,
            StageOrder::Canonical,
        )
        .unwrap();
    }
    assert_eq!(integers_of(&staged(name, Part::Nodes), "node_id"), [-3, 2, 9, 10, 40]);
    assert_eq!(integers_of(&staged(name, Part::Edges), "source"), [-3, 9, 9, 9, 10]);
    assert_eq!(integers_of(&staged(name, Part::Edges), "target"), [9, 2, 2, 10, 9]);
    // Nodes derived from Int64 edges are Int64, in the same order rule.
    Registry::stage(name, Part::Nodes, &[], &integer, true, StageOrder::Canonical).unwrap();
    assert_eq!(
        integers_of(&Registry::node_batches(name).unwrap().0, "node_id"),
        [-3, 2, 9, 10]
    );
    assert!(Registry::drop(name).unwrap());

    // The same graph as staged, once with each identity: every kernel's rows
    // agree, ids included, because they are the same text.
    let source = |id: &str| {
        let mut options = no_options();
        options.insert("source".into(), id.into());
        options
    };
    let weighted = {
        let mut options = source("9");
        options.insert("weightProperty".into(), "w".into());
        options
    };
    let calls = [
        ("wcc", no_options()),
        ("degree", no_options()),
        ("pagerank", no_options()),
        ("bfs", source("-3")),
        ("dijkstra", weighted),
    ];
    let mut answers = Vec::new();
    for (label, mapping) in [("text", &text), ("int64", &integer)] {
        let name = format!("identity-{label}");
        for (part, batch) in [(Part::Nodes, &nodes), (Part::Edges, &links)] {
            Registry::stage(
                &name,
                part,
                std::slice::from_ref(batch),
                mapping,
                true,
                StageOrder::AsStaged,
            )
            .unwrap();
        }
        answers.push(
            calls
                .iter()
                .map(|(algorithm, options)| {
                    rendered(run(algorithm, &name, &validate(algorithm, options).unwrap()))
                })
                .collect::<Vec<_>>(),
        );
        assert!(Registry::drop(&name).unwrap());
    }
    for (index, (algorithm, _)) in calls.iter().enumerate() {
        assert!(!answers[0][index][0].starts_with("error"), "{algorithm}: {:?}", answers[0][index]);
        assert_eq!(answers[0][index], answers[1][index], "{algorithm}");
    }

    // Integer nodes with text edges still build: the integer side is read as text.
    let name = "identity-mixed";
    Registry::stage(name, Part::Nodes, std::slice::from_ref(&nodes), &integer, true, StageOrder::AsStaged)
        .unwrap();
    Registry::stage(name, Part::Edges, std::slice::from_ref(&links), &text, true, StageOrder::AsStaged)
        .unwrap();
    let mixed = rendered(run("wcc", name, &validate("wcc", &no_options()).unwrap()));
    assert_eq!(mixed, answers[0][0]);
    assert!(Registry::drop(name).unwrap());
}

fn staged(name: &str, part: Part) -> Vec<RecordBatch> {
    let entry = Registry::entry(name).unwrap().unwrap();
    let e = entry.read().unwrap();
    match part {
        Part::Nodes => e.nodes.clone(),
        Part::Edges => e.edges.clone(),
    }
}

fn strings_of(batches: &[RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let c = b
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            (0..c.len()).map(move |i| c.value(i).to_string())
        })
        .collect()
}

/// Ids are Utf8 once staged, so canonical order is text order: `"10"` sorts
/// before `"9"`, for staged nodes and for nodes derived from edges alike.
/// Parallel edges with no `edge_id` are ordered by their weight.
#[test]
fn canonical_order_is_text_order_on_ids_and_breaks_ties_by_the_other_columns() {
    let name = "text-order";
    let mapping = ColumnMapping::default();
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![9, 10, 2]))],
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Nodes,
        &[nodes],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    assert_eq!(
        strings_of(&staged(name, Part::Nodes), "node_id"),
        ["10", "2", "9"]
    );
    let batch = edges(
        &["9", "9", "10", "9"],
        &["2", "2", "9", "10"],
        Some(&[3.0, 1.0, 5.0, 2.0]),
    );
    Registry::stage(
        name,
        Part::Edges,
        std::slice::from_ref(&batch),
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let sorted = staged(name, Part::Edges);
    assert_eq!(strings_of(&sorted, "source"), ["10", "9", "9", "9"]);
    assert_eq!(strings_of(&sorted, "target"), ["9", "10", "2", "2"]);
    let w = sorted[0]
        .column_by_name("property.w")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(w.values().to_vec(), [5.0, 2.0, 1.0, 3.0]);

    Registry::stage(
        name,
        Part::Nodes,
        &[],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    assert_eq!(
        strings_of(&Registry::node_batches(name).unwrap().0, "node_id"),
        ["10", "2", "9"]
    );
    // As staged, rows keep their arrival order, and derived nodes the order
    // their ids first appear.
    Registry::stage(
        name,
        Part::Edges,
        &[batch],
        &mapping,
        true,
        StageOrder::AsStaged,
    )
    .unwrap();
    assert_eq!(
        strings_of(&staged(name, Part::Edges), "source"),
        ["9", "9", "10", "9"]
    );
    assert_eq!(
        strings_of(&Registry::node_batches(name).unwrap().0, "node_id"),
        ["9", "2", "10"]
    );
    assert!(Registry::drop(name).unwrap());
}

/// Appends of differently shaped rows are sorted as one part: a batch without
/// the weight column is filled as Grust reads a missing column (absent), so
/// the two appends in either order stage the same rows. A column that is two
/// types in two appends has no single sorted form and is refused, naming the
/// opt-out, which accepts it.
#[test]
fn appends_of_different_shapes_are_sorted_as_one_part() {
    let mapping = ColumnMapping::default();
    let weighted = edges(&["b", "a"], &["c", "b"], Some(&[2.0, 1.0]));
    let bare = edges(&["a", "c"], &["c", "a"], None);
    let mut seen = Vec::new();
    for (name, batches) in [
        ("shapes-1", [&weighted, &bare]),
        ("shapes-2", [&bare, &weighted]),
    ] {
        Registry::stage(
            name,
            Part::Edges,
            &[batches[0].clone()],
            &mapping,
            true,
            StageOrder::Canonical,
        )
        .unwrap();
        Registry::stage(
            name,
            Part::Edges,
            &[batches[1].clone()],
            &mapping,
            false,
            StageOrder::Canonical,
        )
        .unwrap();
        let mut options = no_options();
        options.insert("weightProperty".into(), serde_json::json!("w"));
        options.insert("defaultWeight".into(), serde_json::json!(7.0));
        let degree = rendered(run("degree", name, &validate("degree", &options).unwrap()));
        seen.push((staged(name, Part::Edges), degree));
        assert!(Registry::drop(name).unwrap());
    }
    assert_eq!(seen[0], seen[1]);
    let present = seen[0].0[0]
        .column_by_name("present.w")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    // a→b (w 1), a→c (none), b→c (w 2), c→a (none).
    assert_eq!(present, [Some(true), Some(false), Some(true), Some(false)]);

    let name = "shapes-conflict";
    let integer = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("src", DataType::Utf8, false),
            Field::new("dst", DataType::Utf8, false),
            Field::new("w", DataType::Int32, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(Int32Array::from(vec![1])),
        ],
    )
    .unwrap();
    Registry::stage(
        name,
        Part::Edges,
        &[weighted],
        &mapping,
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let error = Registry::stage(
        name,
        Part::Edges,
        std::slice::from_ref(&integer),
        &mapping,
        false,
        StageOrder::Canonical,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("property.w") && error.contains("asStaged"),
        "{error}"
    );
    // The refused write left the rows staged before it.
    assert_eq!(strings_of(&staged(name, Part::Edges), "source"), ["a", "b"]);
    let staged_rows = Registry::stage(
        name,
        Part::Edges,
        &[integer],
        &mapping,
        false,
        StageOrder::AsStaged,
    )
    .unwrap();
    assert_eq!(staged_rows, 3);
    assert!(Registry::drop(name).unwrap());
}

#[test]
fn the_order_option_parses_in_either_case() {
    assert_eq!(
        StageOrder::parse("canonical").unwrap(),
        StageOrder::Canonical
    );
    assert_eq!(StageOrder::parse("asStaged").unwrap(), StageOrder::AsStaged);
    assert_eq!(
        StageOrder::parse(" ASSTAGED ").unwrap(),
        StageOrder::AsStaged
    );
    assert_eq!(StageOrder::default(), StageOrder::Canonical);
    let error = StageOrder::parse("sorted").unwrap_err().to_string();
    assert!(
        error.contains(ORDER_OPTION) && error.contains("asStaged"),
        "{error}"
    );
}

// ---- The memory budget over staging, sorting and projections ----

/// `rows` edges `n{i}` → `n{(i * 7 + 1) % rows}` numbered from `first`,
/// weighted, in reverse id order so a canonical write has sorting to do.
fn budget_edges(first: usize, rows: usize) -> RecordBatch {
    let ids: Vec<usize> = (first..first + rows).rev().collect();
    let source: Vec<String> = ids.iter().map(|i| format!("n{i:08}")).collect();
    let target: Vec<String> = ids
        .iter()
        .map(|i| format!("n{:08}", first + (i * 7 + 1) % rows))
        .collect();
    let source: Vec<&str> = source.iter().map(String::as_str).collect();
    let target: Vec<&str> = target.iter().map(String::as_str).collect();
    let weight: Vec<f64> = ids.iter().map(|i| (i % 5) as f64).collect();
    edges(&source, &target, Some(&weight))
}

/// What one write costs at its peak, and what it holds after, measured on a
/// budget that cannot refuse it.
fn cost(batch: &RecordBatch, order: StageOrder) -> (usize, usize) {
    let store = Store::new(usize::MAX);
    store
        .stage(
            "cost",
            Part::Edges,
            std::slice::from_ref(batch),
            &ColumnMapping::default(),
            true,
            order,
        )
        .unwrap();
    let memory = store.memory().unwrap();
    (memory.peak_bytes, memory.used_bytes)
}

fn part_of(store: &Store, name: &str) -> Vec<RecordBatch> {
    let entry = store.entry(name).unwrap().unwrap();
    let e = entry.read().unwrap();
    e.edges.clone()
}

fn info_of(store: &Store, name: &str) -> Option<GraphInfo> {
    store.list().unwrap().into_iter().find(|g| g.name == name)
}

/// A write that does not fit is refused with the graph, the part and the
/// sizes named, and the graph is exactly as it was: the same rows, the same
/// revision, its projection still cached, the same bytes held. A refused
/// write to a new graph leaves no graph behind, and a refused replace keeps
/// the rows it would have replaced.
#[test]
fn a_write_over_the_budget_is_refused_and_leaves_the_graph_as_it_was() {
    let small = budget_edges(0, 200);
    let large = budget_edges(1_000, 20_000);
    let (small_peak, small_held) = cost(&small, StageOrder::Canonical);
    let (large_peak, _) = cost(&large, StageOrder::Canonical);
    assert!(large_peak > 20 * small_peak, "{large_peak} vs {small_peak}");
    // Room for the small graph and its projection, with plenty to spare,
    // and far less than the large write.
    let limit = 10 * small_peak;
    let store = Store::new(limit);
    let mapping = ColumnMapping::default();
    let name = "refused";
    store
        .stage(
            name,
            Part::Edges,
            &[small],
            &mapping,
            true,
            StageOrder::Canonical,
        )
        .unwrap();
    let projection = store
        .projection(name, &validate("wcc", &no_options()).unwrap())
        .unwrap();
    drop(projection);
    let before_rows = part_of(&store, name);
    let before = info_of(&store, name).unwrap();
    let used_before = store.memory().unwrap().used_bytes;
    assert_eq!(before.staged_bytes, small_held);
    assert_eq!(before.projections, 1);
    assert!(
        used_before > before.staged_bytes,
        "the projection is admitted too"
    );

    for (replace, order) in [
        (false, StageOrder::Canonical),
        (true, StageOrder::Canonical),
        (false, StageOrder::AsStaged),
        (true, StageOrder::AsStaged),
    ] {
        let error = store
            .stage(
                name,
                Part::Edges,
                std::slice::from_ref(&large),
                &mapping,
                replace,
                order,
            )
            .unwrap_err();
        let message = error.to_string();
        assert!(
            matches!(error, DataFusionError::ResourcesExhausted(_)),
            "{message}"
        );
        for expected in [
            "`refused`",
            "its edges",
            &format!("{limit}-byte"),
            MEMORY_BYTES_VARIABLE,
        ] {
            assert!(message.contains(expected), "{expected} in {message}");
        }
        let after = info_of(&store, name).unwrap();
        assert_eq!(
            part_of(&store, name),
            before_rows,
            "replace: {replace}, {order:?}"
        );
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.staged_edges, 200);
        assert_eq!(after.staged_bytes, before.staged_bytes);
        assert_eq!(after.projections, 1, "the cached projection survives");
        assert_eq!(store.memory().unwrap().used_bytes, used_before);
    }

    let error = store
        .stage(
            "never-staged",
            Part::Edges,
            &[large],
            &mapping,
            true,
            StageOrder::Canonical,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("`never-staged`"), "{error}");
    assert!(info_of(&store, "never-staged").is_none());
    assert_eq!(store.memory().unwrap().used_bytes, used_before);
}

/// Canonical order sorts into a second copy of the part, beside the
/// permutation and the sort keys; all of that is admitted before the sort
/// starts. A write whose rows fit but whose sort does not is refused under
/// canonical order, naming the sort and the opt-out, and admitted as staged.
/// Once sorted, the part holds its copy, not the working space.
#[test]
fn the_sorts_working_space_is_admitted_before_the_sort() {
    let batch = budget_edges(0, 5_000);
    let (as_staged, as_staged_held) = cost(&batch, StageOrder::AsStaged);
    let (canonical, canonical_held) = cost(&batch, StageOrder::Canonical);
    // Normalization now admits its conversion/building workspace before it
    // allocates, and returns that conservative excess afterwards. Retained
    // bytes still equal the actual Arrow buffers (checked below).
    assert!(as_staged > as_staged_held);
    // Sorted, it also needs the copy and the permutation at once.
    assert!(
        canonical > as_staged + as_staged_held / 2,
        "{canonical} vs {as_staged}"
    );
    let limit = (as_staged + canonical) / 2;
    let store = Store::new(limit);
    let mapping = ColumnMapping::default();
    let error = store
        .stage(
            "sorted",
            Part::Edges,
            std::slice::from_ref(&batch),
            &mapping,
            true,
            StageOrder::Canonical,
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("sort's working space") && error.contains("asStaged"),
        "{error}"
    );
    assert!(info_of(&store, "sorted").is_none());
    assert_eq!(store.memory().unwrap().used_bytes, 0);
    let rows = store
        .stage(
            "sorted",
            Part::Edges,
            std::slice::from_ref(&batch),
            &mapping,
            true,
            StageOrder::AsStaged,
        )
        .unwrap();
    assert_eq!(rows, 5_000);
    assert_eq!(
        info_of(&store, "sorted").unwrap().staged_bytes,
        as_staged_held
    );

    // What a sorted part is charged is what it keeps alive: the bound its
    // copy was admitted under is shrunk to the copy's size once made.
    let sorted = Store::new(usize::MAX);
    sorted
        .stage(
            "s",
            Part::Edges,
            &[batch],
            &mapping,
            true,
            StageOrder::Canonical,
        )
        .unwrap();
    let held = held_bytes(&part_of(&sorted, "s"));
    assert_eq!(info_of(&sorted, "s").unwrap().staged_bytes, canonical_held);
    assert_eq!(canonical_held, held);
}

/// Bytes are measured as the allocations the rows keep alive: a slice keeps
/// its whole buffer, and buffers shared between slices count once.
#[test]
fn held_bytes_counts_each_allocation_once_at_its_capacity() {
    let batch = budget_edges(0, 1_000);
    let whole = held_bytes(std::slice::from_ref(&batch));
    assert!(whole >= batch.get_array_memory_size() / 2, "{whole}");
    let halves = [batch.slice(0, 500), batch.slice(500, 500)];
    assert_eq!(held_bytes(&halves), whole);
    assert_eq!(held_bytes(&[batch.slice(0, 10)]), whole);
    assert_eq!(held_bytes(&[batch.clone(), batch.clone()]), whole);

    // Staged as they are, ten rows sliced from a thousand keep the thousand;
    // sorted, they are copied compactly and keep only themselves.
    let store = Store::new(usize::MAX);
    let mapping = ColumnMapping::default();
    let slice = batch.slice(0, 10);
    store
        .stage(
            "slice",
            Part::Edges,
            std::slice::from_ref(&slice),
            &mapping,
            true,
            StageOrder::AsStaged,
        )
        .unwrap();
    let kept = info_of(&store, "slice").unwrap().staged_bytes;
    assert!(kept >= whole / 2, "{kept} of {whole}");
    store
        .stage(
            "slice",
            Part::Edges,
            &[slice],
            &mapping,
            true,
            StageOrder::Canonical,
        )
        .unwrap();
    let sorted = info_of(&store, "slice").unwrap().staged_bytes;
    assert!(sorted < whole / 5, "{sorted} of {whole}");
}

/// Replacing a part, restaging (which evicts the graph's cached
/// projections) and dropping a graph each return their bytes; a projection
/// still held by a running read keeps its bytes until that read lets go. With
/// the bytes back, a write the budget refused is admitted.
#[test]
fn drop_and_restage_return_their_bytes_and_the_budget_recovers() {
    let first = budget_edges(0, 4_000);
    let second = budget_edges(10_000, 4_000);
    let (peak, held) = cost(&first, StageOrder::Canonical);
    let mapping = ColumnMapping::default();
    let wcc = validate("wcc", &no_options()).unwrap();
    let stage = |store: &Store, name: &str, batch: &RecordBatch, replace: bool| {
        store.stage(
            name,
            Part::Edges,
            std::slice::from_ref(batch),
            &mapping,
            replace,
            StageOrder::Canonical,
        )
    };

    // Room for one such graph, not for two.
    let store = Store::new(peak + held / 2);
    stage(&store, "one", &first, true).unwrap();
    assert_eq!(store.memory().unwrap().used_bytes, held);
    let error = stage(&store, "two", &second, true).unwrap_err().to_string();
    assert!(error.contains("`two`"), "{error}");
    // Reserve the now-idle conservative staging workspace as another consumer.
    // Whether CSR fits must not depend on the sort bound being smaller than
    // projection construction; both draw from this same pool.
    let occupied = store
        .pool
        .reserve(store.pool.limits().memory_bytes - held - 1)
        .unwrap();
    let error = store
        .projection("one", &wcc)
        .err()
        .expect("refused")
        .to_string();
    assert!(
        error.contains("`one`") && error.contains("projection"),
        "{error}"
    );
    drop(occupied);
    assert_eq!(store.memory().unwrap().used_bytes, held);
    assert!(store.drop("one").unwrap());
    assert_eq!(store.memory().unwrap().used_bytes, 0);
    assert_eq!(store.memory().unwrap().staged_bytes, 0);
    stage(&store, "two", &second, true).unwrap();
    assert_eq!(info_of(&store, "two").unwrap().staged_edges, 4_000);

    // A projection is admitted from the same budget and released on restage.
    let store = Store::new(usize::MAX);
    let stage =
        |name: &str, batch: &RecordBatch, replace: bool| stage(&store, name, batch, replace);
    stage("one", &first, true).unwrap();
    let projection = store.projection("one", &wcc).unwrap();
    let with_projection = store.memory().unwrap().used_bytes;
    assert!(with_projection > held);
    stage("one", &first, true).unwrap();
    assert_eq!(info_of(&store, "one").unwrap().projections, 0);
    // The read still holding the old projection keeps its bytes...
    assert_eq!(store.memory().unwrap().used_bytes, with_projection);
    drop(projection);
    // ...until it lets go.
    assert_eq!(store.memory().unwrap().used_bytes, held);

    // Replacing with fewer rows holds fewer bytes.
    stage("one", &first.slice(0, 1_000), true).unwrap();
    let smaller = store.memory().unwrap().used_bytes;
    assert!(smaller < held / 2, "{smaller} vs {held}");
    assert_eq!(info_of(&store, "one").unwrap().staged_bytes, smaller);
    // An empty replace holds nothing, and the graph stays listed.
    store
        .stage(
            "one",
            Part::Edges,
            &[],
            &mapping,
            true,
            StageOrder::Canonical,
        )
        .unwrap();
    assert_eq!(store.memory().unwrap().used_bytes, 0);
    assert!(info_of(&store, "one").is_some());
    // Dropping a graph releases its rows and its cached projections.
    stage("one", &first, true).unwrap();
    store.projection("one", &wcc).unwrap();
    assert!(store.memory().unwrap().used_bytes > held);
    assert!(store.drop("one").unwrap());
    assert_eq!(store.memory().unwrap().used_bytes, 0);
}

/// A write fed a batch at a time, as Sail's sink feeds it, is refused at the
/// batch that crosses the budget, before the rest arrive; the graph is
/// untouched until `finish`. The handle can be held across an `await`.
#[test]
fn a_streamed_write_is_refused_at_the_batch_that_crosses_the_budget() {
    fn send<T: Send>(_: &T) {}
    send(&Registry::staging(
        "unused",
        Part::Edges,
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    ));
    let batch = budget_edges(0, 1_000);
    let (peak, held) = cost(&batch, StageOrder::AsStaged);
    // Room for two retained batches and the third's conversion workspace, but
    // not for three retained batches plus the fourth's conversion workspace.
    let store = Store::new(2 * held + peak + held / 2);
    let mapping = ColumnMapping::default();
    let mut staging = store.staging(
        "streamed",
        Part::Edges,
        &mapping,
        true,
        StageOrder::AsStaged,
    );
    for _ in 0..3 {
        staging.push(&batch).unwrap();
        assert!(info_of(&store, "streamed").is_none());
    }
    let error = staging.push(&batch).unwrap_err().to_string();
    assert!(
        error.contains("`streamed`") && error.contains("1000 more rows"),
        "{error}"
    );
    assert_eq!(store.memory().unwrap().used_bytes, 3 * held);
    drop(staging);
    assert_eq!(store.memory().unwrap().used_bytes, 0);
    let mut staging = store.staging(
        "streamed",
        Part::Edges,
        &mapping,
        true,
        StageOrder::AsStaged,
    );
    for _ in 0..3 {
        staging.push(&batch).unwrap();
    }
    assert_eq!(staging.finish().unwrap(), 3_000);
    assert_eq!(info_of(&store, "streamed").unwrap().staged_bytes, 3 * held);
}

/// The graph listing reports each graph's staged bytes, and `nutmeg_memory()`
/// the budget, what is in use and the staged total, through SQL.
#[tokio::test]
async fn the_listing_reports_bytes_per_graph_and_in_total() -> Result<()> {
    use arrow::datatypes::Int64Type;
    let name = "listed-bytes";
    Registry::stage(
        name,
        Part::Edges,
        &[budget_edges(0, 100)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )?;
    let staged = Registry::list()?
        .into_iter()
        .find(|g| g.name == name)
        .unwrap()
        .staged_bytes;
    assert!(staged > 0);
    let ctx = SessionContext::new();
    register(&ctx);
    let batches = ctx
        .sql(&format!(
            "SELECT \"stagedBytes\" FROM nutmeg_graphs() WHERE name = '{name}'"
        ))
        .await?
        .collect()
        .await?;
    let listed = batches[0].column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!(listed as usize, staged);
    let batches = ctx
        .sql(
            "SELECT \"limitBytes\", \"usedBytes\", \"stagedBytes\", \"peakBytes\" \
             FROM nutmeg_memory()",
        )
        .await?
        .collect()
        .await?;
    let value = |c: usize| batches[0].column(c).as_primitive::<Int64Type>().value(0) as usize;
    assert_eq!(value(0), memory_bytes());
    // Other tests stage and drop at the same time, so only the order holds.
    assert!(
        value(2) > 0 && value(1) >= value(2) && value(3) >= value(1),
        "{batches:?}"
    );
    assert!(Registry::drop(name)?);
    Ok(())
}

/// Writes to different graphs, from many threads at once, can never hold
/// more than the budget together. Each thread stages its own graph (sorted,
/// so each write has a transient peak), checks it, and drops it, over and
/// over; the budget has room for about two such graphs, so many writes are
/// refused. A monitor measures, while they run, the bytes the staged rows
/// actually keep alive, and checks them against what each graph was charged,
/// and the charges against the budget.
#[test]
fn concurrent_writes_to_different_graphs_never_exceed_the_budget() {
    const THREADS: usize = 8;
    const ROUNDS: usize = 40;
    let batch = budget_edges(0, 2_000);
    let (peak, held) = cost(&batch, StageOrder::Canonical);
    let limit = peak + held + held / 2;
    let store = Store::new(limit);
    let done = std::sync::atomic::AtomicBool::new(false);
    let (admitted, refused) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let most_held = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let monitor = scope.spawn(|| {
            let mut samples = 0usize;
            while !done.load(Ordering::Acquire) {
                let mut measured = 0usize;
                let mut charged = 0usize;
                for graph in store.list().unwrap() {
                    let Some(entry) = store.entry(&graph.name).unwrap() else {
                        continue;
                    };
                    let e = entry.read().unwrap();
                    let keeps = held_bytes(&e.edges);
                    let charge = e.edge_bytes.bytes();
                    assert!(
                        keeps <= charge,
                        "{}: keeps {keeps}, charged {charge}",
                        graph.name
                    );
                    measured += keeps;
                    charged += charge;
                }
                assert!(measured <= limit, "staged rows keep {measured} of {limit}");
                assert!(charged <= limit, "charged {charged} of {limit}");
                let used = store.memory().unwrap().used_bytes;
                assert!(used <= limit, "{used} of {limit}");
                most_held.fetch_max(measured, Ordering::Relaxed);
                samples += 1;
                std::thread::yield_now();
            }
            samples
        });
        let writers: Vec<_> = (0..THREADS)
            .map(|t| {
                let (store, batch) = (&store, &batch);
                let (admitted, refused) = (&admitted, &refused);
                scope.spawn(move || {
                    let name = format!("concurrent-{t}");
                    for _ in 0..ROUNDS {
                        match store.stage(
                            &name,
                            Part::Edges,
                            std::slice::from_ref(batch),
                            &ColumnMapping::default(),
                            true,
                            StageOrder::Canonical,
                        ) {
                            Ok(rows) => {
                                assert_eq!(rows, 2_000);
                                admitted.fetch_add(1, Ordering::Relaxed);
                                std::thread::yield_now();
                                assert!(store.drop(&name).unwrap());
                            }
                            Err(error) => {
                                assert!(
                                    matches!(error, DataFusionError::ResourcesExhausted(_)),
                                    "{error}"
                                );
                                assert!(store.entry(&name).unwrap().is_none());
                                refused.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        done.store(true, Ordering::Release);
        assert!(monitor.join().unwrap() > 0);
    });
    let (admitted, refused) = (admitted.into_inner(), refused.into_inner());
    assert_eq!(admitted + refused, THREADS * ROUNDS);
    assert!(admitted > 0, "no write was admitted");
    assert!(
        refused > 0,
        "no write was refused, so the budget was never contended"
    );
    let memory = store.memory().unwrap();
    assert!(memory.peak_bytes <= limit, "{memory:?}");
    assert_eq!(memory.used_bytes, 0, "every byte was returned: {memory:?}");
    assert!(most_held.into_inner() <= limit);
}

/// A sorted part is charged what its copy keeps alive, not the bound the copy
/// was admitted under: the bound is shrunk once the copy is made, and the
/// budget shows the real size. The peak keeps the bound, which was admitted.
#[test]
fn a_sorted_part_returns_what_its_bound_overestimated() {
    let batch = budget_edges(0, 5_000);
    let store = Store::new(usize::MAX);
    store
        .stage(
            "shrunk",
            Part::Edges,
            std::slice::from_ref(&batch),
            &ColumnMapping::default(),
            true,
            StageOrder::Canonical,
        )
        .unwrap();
    let part = part_of(&store, "shrunk");
    let held = held_bytes(&part);
    let bound = sorted_copy_bound(&part);
    assert!(
        bound > held,
        "the bound {bound} would return nothing of {held}"
    );
    eprintln!(
        "sorted copy: bound {bound}, held {held}, returned {}",
        bound - held
    );
    let entry = store.entry("shrunk").unwrap().unwrap();
    assert_eq!(entry.read().unwrap().edge_bytes.bytes(), held);
    let memory = store.memory().unwrap();
    assert_eq!(memory.used_bytes, held, "{memory:?}");
    assert_eq!(memory.staged_bytes, held, "{memory:?}");
    assert!(memory.peak_bytes >= bound, "{memory:?}");
    drop((part, entry));
    assert!(store.drop("shrunk").unwrap());
    assert_eq!(store.memory().unwrap().used_bytes, 0);
}

/// A directed graph of `nodes` nodes with between one and `degree` out-edges
/// each, staged as `name`. Degrees in and out vary from node to node, so the
/// uniform ranks are not PageRank's fixed point and [`pagerank_for`] runs
/// every iteration it is given rather than converging at once, as it does on
/// a regular graph.
fn stage_read_graph(store: &Store, name: &str, nodes: usize, degree: usize) {
    let mut source = Vec::with_capacity(nodes * degree);
    let mut target = Vec::with_capacity(nodes * degree);
    for i in 0..nodes {
        for j in 0..1 + i % degree {
            source.push(format!("n{i:06}"));
            target.push(format!(
                "n{:06}",
                (i * i * 31 + j * 7_919 + i / 7 + 1) % nodes
            ));
        }
    }
    let source: Vec<&str> = source.iter().map(String::as_str).collect();
    let target: Vec<&str> = target.iter().map(String::as_str).collect();
    store
        .stage(
            name,
            Part::Edges,
            &[edges(&source, &target, None)],
            &ColumnMapping::default(),
            true,
            StageOrder::Canonical,
        )
        .unwrap();
}

/// PageRank for `iterations` iterations. A tolerance of zero stops early only
/// at an exact fixed point, and a damping near one makes that tens of
/// thousands of iterations away on [`stage_read_graph`]'s graphs, so the
/// iterations are the work. [`iterations_run`] checks it.
fn pagerank_for(iterations: usize) -> ValidatedArguments {
    let options = serde_json::json!({
        "tolerance": 0.0,
        "damping": 0.999,
        "maxIterations": iterations,
    });
    validate("pagerank", options.as_object().unwrap()).unwrap()
}

/// The iterations a PageRank result ran.
fn iterations_run(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column_by_name("iterations")
        .unwrap()
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(0)
}

fn work_of(query: &Query) -> usize {
    query.usage().unwrap().counted_work().unwrap_or(0)
}

const READ_NODES: usize = 20_000;
const READ_DEGREE: usize = 8;
/// The sibling's iterations: long enough to be still running when the
/// victim, started beside it, is cancelled.
const SIBLING_ITERATIONS: usize = 100;
/// The victim's: bounded, so that a victim that cannot be stopped fails the
/// test by finishing rather than hanging it.
const VICTIM_ITERATIONS: usize = 20 * SIBLING_ITERATIONS;

/// One round: two reads of one cached graph at once, the victim cancelled
/// once both are running. Returns whether the sibling was still running when
/// the victim was cancelled.
fn cancel_one_of_two(store: &Store, name: &str, lone: &[RecordBatch]) -> bool {
    let victim = store.query(QueryLimits::default()).unwrap();
    let sibling = store.query(QueryLimits::default()).unwrap();
    let (victim_args, sibling_args) = (
        pagerank_for(VICTIM_ITERATIONS),
        pagerank_for(SIBLING_ITERATIONS),
    );
    let sibling_done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let v = scope.spawn(|| store.run(&victim, "pagerank", name, &victim_args));
        let s = scope.spawn(|| {
            let result = store.run(&sibling, "pagerank", name, &sibling_args);
            sibling_done.store(true, Ordering::Release);
            result
        });
        // Each has charged work of its own once its kernel is running.
        while (work_of(&victim) == 0 || work_of(&sibling) == 0)
            && !v.is_finished()
            && !s.is_finished()
        {
            std::thread::yield_now();
        }
        let overlapped = !sibling_done.load(Ordering::Acquire);
        victim.cancel().unwrap();
        let stopped = v.join().unwrap();
        let finished = s.join().unwrap();
        let error = stopped
            .expect_err("the cancelled read was stopped")
            .to_string();
        assert!(error.contains("cancelled"), "{error}");
        assert_eq!(finished.expect("the sibling finished"), lone);
        store.pool.checkpoint().expect("the pool is not cancelled");
        sibling
            .context
            .checkpoint()
            .expect("the sibling is not cancelled");
        overlapped
    })
}

/// Two reads run at once on one cached projection, each on its own child of
/// the pool. Cancelling one mid-run stops it alone: the other finishes with
/// the result a lone read gives, and neither it nor the pool is cancelled.
/// Every byte either read took is back afterwards. `NUTMEG_CANCEL_ROUNDS`
/// (3 by default) repeats it, for stress runs.
#[test]
fn cancelling_one_read_leaves_a_concurrent_read_and_the_pool_running() {
    let rounds: usize = std::env::var("NUTMEG_CANCEL_ROUNDS")
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(3);
    let store = Store::new(usize::MAX);
    let name = "cancelled";
    stage_read_graph(&store, name, READ_NODES, READ_DEGREE);
    // A lone read builds and caches the projection and its transpose, both
    // the pool's; the rounds then run on the cached graph.
    let lone = store
        .run(
            &store.query(QueryLimits::default()).unwrap(),
            "pagerank",
            name,
            &pagerank_for(SIBLING_ITERATIONS),
        )
        .unwrap();
    assert_eq!(iterations_run(&lone), SIBLING_ITERATIONS as i64);
    let baseline = store.memory().unwrap().used_bytes;
    let mut overlapped = 0;
    for _ in 0..rounds {
        overlapped += usize::from(cancel_one_of_two(&store, name, &lone));
        assert_eq!(store.memory().unwrap().used_bytes, baseline);
    }
    eprintln!("cancel isolation: {rounds} rounds, sibling still running at {overlapped}");
    assert!(overlapped > 0, "the sibling never ran beside the victim");
    // A read after all of that runs as the first did.
    let after = store
        .run(
            &store.query(QueryLimits::default()).unwrap(),
            "pagerank",
            name,
            &pagerank_for(SIBLING_ITERATIONS),
        )
        .unwrap();
    assert_eq!(after, lone);
}

/// Reads draw on the one budget: however many run at once, what they hold
/// with the staged rows and the cached projection never exceeds
/// `NUTMEG_MEMORY_BYTES`. A read that does not fit is refused, and every
/// read that fits returns what a lone read does.
#[test]
fn concurrent_reads_never_exceed_the_budget_together() {
    const THREADS: usize = 8;
    const ROUNDS: usize = 30;
    let name = "shared-budget";
    let args = pagerank_for(20);
    // Measured on a budget that cannot refuse: what the graph holds with
    // its projection and transpose built, and what one read holds at most.
    let measure = Store::new(usize::MAX);
    stage_read_graph(&measure, name, 3_000, 4);
    let query = measure.query(QueryLimits::default()).unwrap();
    let lone = measure.run(&query, "pagerank", name, &args).unwrap();
    let baseline = measure.memory().unwrap().used_bytes;
    let per_read = query.usage().unwrap().peak_bytes;
    assert!(per_read > 0);
    drop(query);
    // Room for the graph and two and a half reads, and for building it.
    let limit = (baseline + 2 * per_read + per_read / 2).max(measure.memory().unwrap().peak_bytes);
    let store = Store::new(limit);
    stage_read_graph(&store, name, 3_000, 4);
    let first = store
        .run(
            &store.query(QueryLimits::default()).unwrap(),
            "pagerank",
            name,
            &args,
        )
        .unwrap();
    assert_eq!(first, lone);
    assert_eq!(store.memory().unwrap().used_bytes, baseline);
    // Setup now reserves conservative conversion/sort bounds. Occupy that
    // transient headroom after setup so eight readers still contend for two
    // and a half reads' room, independent of setup's higher peak admission.
    let occupied = store
        .pool
        .reserve(limit - (baseline + 2 * per_read + per_read / 2))
        .unwrap();
    let done = std::sync::atomic::AtomicBool::new(false);
    let (admitted, refused) = (AtomicUsize::new(0), AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let monitor = scope.spawn(|| {
            let mut samples = 0usize;
            while !done.load(Ordering::Acquire) {
                let used = store.memory().unwrap().used_bytes;
                assert!(used <= limit, "{used} of {limit}");
                samples += 1;
                std::thread::yield_now();
            }
            samples
        });
        let readers: Vec<_> = (0..THREADS)
            .map(|_| {
                let (store, args, lone) = (&store, &args, &lone);
                let (admitted, refused) = (&admitted, &refused);
                scope.spawn(move || {
                    for _ in 0..ROUNDS {
                        let query = store.query(QueryLimits::default()).unwrap();
                        match store.run(&query, "pagerank", name, args) {
                            Ok(batches) => {
                                assert_eq!(&batches, lone);
                                admitted.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(error) => {
                                assert!(
                                    matches!(error, DataFusionError::ResourcesExhausted(_)),
                                    "{error}"
                                );
                                let error = error.to_string();
                                assert!(error.contains(MEMORY_BYTES_VARIABLE), "{error}");
                                refused.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                })
            })
            .collect();
        for reader in readers {
            reader.join().unwrap();
        }
        done.store(true, Ordering::Release);
        assert!(monitor.join().unwrap() > 0);
    });
    let (admitted, refused) = (admitted.into_inner(), refused.into_inner());
    eprintln!("shared budget: {admitted} reads admitted, {refused} refused");
    assert_eq!(admitted + refused, THREADS * ROUNDS);
    assert!(admitted > 0, "no read was admitted");
    assert!(
        refused > 0,
        "no read was refused, so the budget was never contended"
    );
    drop(occupied);
    let memory = store.memory().unwrap();
    assert!(memory.peak_bytes <= limit, "{memory:?}");
    assert_eq!(memory.used_bytes, baseline, "every read's bytes returned");
    store.pool.checkpoint().unwrap();
}

/// A read's own deadline, work budget and memory limit stop that read, and
/// only it: a read running beside it on the same cached projection finishes
/// with the lone result, and the pool is untouched.
#[test]
fn a_reads_own_limits_stop_it_alone() {
    let store = Store::new(usize::MAX);
    let name = "limited";
    stage_read_graph(&store, name, READ_NODES, READ_DEGREE);
    let sibling_args = pagerank_for(SIBLING_ITERATIONS);
    let lone = store
        .run(
            &store.query(QueryLimits::default()).unwrap(),
            "pagerank",
            name,
            &sibling_args,
        )
        .unwrap();
    assert_eq!(iterations_run(&lone), SIBLING_ITERATIONS as i64);
    let baseline = store.memory().unwrap().used_bytes;
    let cases = [
        (
            QueryLimits {
                timeout: Some(Duration::from_millis(20)),
                ..QueryLimits::default()
            },
            // Far more than 20ms of work on any machine.
            pagerank_for(100 * VICTIM_ITERATIONS),
            "timed out",
        ),
        (
            QueryLimits {
                work_units: Some(10_000),
                ..QueryLimits::default()
            },
            pagerank_for(VICTIM_ITERATIONS),
            "work budget exceeded",
        ),
        (
            QueryLimits {
                memory_bytes: Some(1_024),
                ..QueryLimits::default()
            },
            pagerank_for(VICTIM_ITERATIONS),
            "1024-byte memory limit (`memoryLimitBytes`)",
        ),
    ];
    for (limits, args, expected) in cases {
        let limited = store.query(limits).unwrap();
        let sibling = store.query(QueryLimits::default()).unwrap();
        let (stopped, finished) = std::thread::scope(|scope| {
            let l = scope.spawn(|| store.run(&limited, "pagerank", name, &args));
            let s = scope.spawn(|| store.run(&sibling, "pagerank", name, &sibling_args));
            (l.join().unwrap(), s.join().unwrap())
        });
        let error = stopped.expect_err(expected).to_string();
        assert!(error.contains(expected), "{limits:?}: {error}");
        assert_eq!(finished.unwrap(), lone, "{limits:?}");
        sibling.context.checkpoint().unwrap();
        store.pool.checkpoint().unwrap();
        assert_eq!(store.memory().unwrap().used_bytes, baseline);
    }
}

/// The limits are read options, taken out before Grust validates the rest,
/// in any case and as numbers or text, since data source options arrive as
/// lowercased strings. A table carries them to every scan.
#[test]
fn read_limits_are_read_options() {
    let mut options = serde_json::json!({
        "timeoutms": "250",
        "WorkLimit": 7,
        "memoryLimitBytes": "4096",
        "damping": 0.85,
    })
    .as_object()
    .unwrap()
    .clone();
    let limits = QueryLimits::take(&mut options).unwrap();
    assert_eq!(
        limits,
        QueryLimits {
            timeout: Some(Duration::from_millis(250)),
            work_units: Some(7),
            memory_bytes: Some(4_096),
            concurrency: None,
        }
    );
    assert_eq!(options.keys().collect::<Vec<_>>(), ["damping"]);
    validate("pagerank", &options).unwrap();
    for bad in [
        serde_json::json!("-1"),
        serde_json::json!(1.5),
        serde_json::json!("soon"),
    ] {
        let mut options = serde_json::Map::new();
        options.insert(TIMEOUT_OPTION.into(), bad.clone());
        let error = QueryLimits::take(&mut options).unwrap_err().to_string();
        assert!(error.contains(TIMEOUT_OPTION), "{bad}: {error}");
    }
    let mut none = no_options();
    assert_eq!(
        QueryLimits::take(&mut none).unwrap(),
        QueryLimits::default()
    );

    let name = "limited-table";
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b", "c"], &["b", "c", "a"], None)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )
    .unwrap();
    let options = serde_json::json!({ "workLimit": "1" });
    let table = AlgorithmTable::new("pagerank", name.into(), options.as_object().unwrap()).unwrap();
    assert_eq!(table.limits().work_units, Some(1));
    let error = table.batches().unwrap_err().to_string();
    assert!(error.contains("work budget exceeded"), "{error}");
    let open = AlgorithmTable::new("pagerank", name.into(), &no_options()).unwrap();
    assert_eq!(open.batches().unwrap()[0].num_rows(), 3);
    assert!(Registry::drop(name).unwrap());
}

/// `concurrency` is one of those read options, and it is the read's: the
/// kernel reads its worker count from the execution the read runs on, which
/// is this read's child of the pool, not the shared projection's execution.
#[test]
fn concurrency_is_a_read_option_and_reaches_the_read_that_asked_for_it() {
    let mut options = no_options();
    options.insert(CONCURRENCY_OPTION.into(), serde_json::json!(8));
    let mut taken = options.clone();
    assert_eq!(QueryLimits::take(&mut taken).unwrap().concurrency, Some(8));
    assert!(
        taken.is_empty(),
        "the option is consumed, not passed to Grust"
    );

    // Grust's validator would reject it, which is why it is taken out first.
    assert!(validate("degree", &options).is_err());

    // Zero threads is a mistake, not a way to ask for none.
    let mut zero = no_options();
    zero.insert(CONCURRENCY_OPTION.into(), serde_json::json!(0));
    let error = QueryLimits::take(&mut zero).unwrap_err().to_string();
    assert!(error.contains(CONCURRENCY_OPTION), "{error}");

    let name = "concurrency-test";
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b", "c"], &["b", "c", "a"], None)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )
    .unwrap();

    // A read that asks for threads runs its kernel on an execution that has
    // them; one that does not runs the code that predates them.
    let asked = AlgorithmTable::new("degree", name.into(), &options).unwrap();
    assert_eq!(asked.limits().concurrency, Some(8));
    assert_eq!(asked.query().unwrap().context.concurrency(), 8);
    assert_eq!(asked.batches().unwrap()[0].num_rows(), 3);
    let plain = AlgorithmTable::new("degree", name.into(), &no_options()).unwrap();
    assert_eq!(
        plain.query().unwrap().context.concurrency_requested(),
        None,
        "a read that never asked for threads inherits none from the pool"
    );
    assert_eq!(plain.batches().unwrap()[0].num_rows(), 3);

    // Both reads shared the one cached projection: the worker count is the
    // read's, so it is not part of what the projection is keyed by.
    let info = Registry::list()
        .unwrap()
        .into_iter()
        .find(|graph| graph.name == name)
        .unwrap();
    assert_eq!(info.projections, 1);
    assert!(Registry::drop(name).unwrap());
}

/// `precision` is Grust's own option on the two rank kernels, so it reaches
/// the validator like `damping`. What Nutmeg owes it is the schema: the
/// `score` column's Arrow type follows the declaration, so the schema a read
/// reports has to be probed per value rather than per algorithm.
#[test]
fn precision_is_a_read_option_and_the_declared_schema_follows_it() {
    let name = "precision-schema";
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b", "c", "a"], &["b", "c", "a", "c"], None)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )
    .unwrap();

    for algorithm in ["pagerank", "articleRank"] {
        // Both rank kernels declare it, with `f64` as the default; no other
        // kernel does, so nothing else is probed per value.
        let declared = definitions()
            .into_iter()
            .find(|d| short(d) == algorithm)
            .unwrap_or_else(|| panic!("{algorithm} is registered"))
            .options
            .iter()
            .any(|option| option.field.name == PRECISION_OPTION);
        assert!(declared, "{algorithm} declares `{PRECISION_OPTION}`");

        for (asked, expected) in [
            (None, DataType::Float64),
            (Some("f64"), DataType::Float64),
            (Some("f32"), DataType::Float32),
        ] {
            let mut options = no_options();
            if let Some(asked) = asked {
                options.insert(PRECISION_OPTION.into(), serde_json::json!(asked));
            }
            let table = AlgorithmTable::new(algorithm, name.into(), &options)
                .unwrap_or_else(|e| panic!("{algorithm} {asked:?}: {e}"));
            let score = table.schema.field_with_name("score").unwrap();
            assert_eq!(
                score.data_type(),
                &expected,
                "{algorithm} {asked:?}: the reported schema"
            );
            // `read_each` fails the read if a batch disagrees with the
            // reported schema, so this also pins the two together.
            let batches = table.batches().unwrap();
            assert_eq!(batches[0].num_rows(), 3);
            assert_eq!(
                batches[0]
                    .schema()
                    .field_with_name("score")
                    .unwrap()
                    .data_type(),
                &expected,
                "{algorithm} {asked:?}: the batch"
            );
        }
    }

    // A kernel that declares no `precision` is unaffected, and its schema is
    // still cached under its bare name.
    let degree = AlgorithmTable::new("degree", name.into(), &no_options()).unwrap();
    assert_eq!(degree.batches().unwrap()[0].num_rows(), 3);
    assert!(
        SCHEMAS.read().unwrap().contains_key("degree"),
        "a kernel without the option keys on its name alone"
    );
    assert!(
        SCHEMAS
            .read()
            .unwrap()
            .contains_key(&format!("pagerank#{PRECISION_OPTION}=f32")),
        "a kernel with it keys on the value too"
    );

    assert!(Registry::drop(name).unwrap());
}

/// A bad `precision` is refused the way any other bad option value is: by
/// Grust, naming the option, before a single row is returned.
#[test]
fn a_bad_precision_is_refused_like_any_other_bad_option() {
    let name = "precision-bad";
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b", "c"], &["b", "c", "a"], None)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )
    .unwrap();

    for bad in ["f16", "double", "F32", ""] {
        let mut options = no_options();
        options.insert(PRECISION_OPTION.into(), serde_json::json!(bad));
        let error = AlgorithmTable::new("pagerank", name.into(), &options)
            .err()
            .unwrap_or_else(|| panic!("`{bad}` was accepted"))
            .to_string();
        assert!(error.contains(PRECISION_OPTION), "`{bad}`: {error}");
        assert!(
            error.contains("'f64'") && error.contains("'f32'"),
            "`{bad}`: {error}"
        );
    }

    // The same shape as a misspelled option name or a bad `orientation`:
    // an error from the planner that names what was wrong.
    let mut misspelled = no_options();
    misspelled.insert("precison".into(), serde_json::json!("f32"));
    let error = AlgorithmTable::new("pagerank", name.into(), &misspelled)
        .unwrap_err()
        .to_string();
    assert!(error.contains("precison"), "{error}");

    // A non-string value is refused too, by the validator's type check.
    let mut wrong_type = no_options();
    wrong_type.insert(PRECISION_OPTION.into(), serde_json::json!(32));
    assert!(AlgorithmTable::new("pagerank", name.into(), &wrong_type).is_err());

    // And the option arrives from a data source as a lowercased string pair,
    // keeping its spelling and its type.
    let options = options_from_strings(
        "pagerank",
        [(PRECISION_OPTION.to_ascii_lowercase(), "f32".to_string())],
    )
    .unwrap();
    assert_eq!(options[PRECISION_OPTION], serde_json::json!("f32"));

    assert!(Registry::drop(name).unwrap());
}

/// The two precisions are the same algorithm at two widths, so their scores
/// agree to about f32's own resolution. The tolerance below is stated rather
/// than tuned: f32 carries ~7 decimal digits, and the scores here sum to 1
/// over three nodes, so 1e-6 absolute is loose by about an order of
/// magnitude and would still catch a kernel that ran a different iteration.
#[test]
fn f32_and_f64_scores_agree_within_f32_resolution() {
    let name = "precision-agreement";
    Registry::stage(
        name,
        Part::Edges,
        &[edges(
            &["a", "b", "c", "a", "b"],
            &["b", "c", "a", "c", "a"],
            None,
        )],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )
    .unwrap();

    let scores = |precision: &str| -> Vec<f64> {
        let mut options = no_options();
        options.insert(PRECISION_OPTION.into(), serde_json::json!(precision));
        let table = AlgorithmTable::new("pagerank", name.into(), &options).unwrap();
        let batches = table.batches().unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let column = batch.column_by_name("score").unwrap();
            match column.data_type() {
                DataType::Float64 => out.extend(
                    column
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .iter()
                        .map(|v| v.unwrap()),
                ),
                DataType::Float32 => out.extend(
                    column
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap()
                        .iter()
                        .map(|v| v.unwrap() as f64),
                ),
                other => panic!("score is {other}"),
            }
        }
        out
    };

    let wide = scores("f64");
    let narrow = scores("f32");
    assert_eq!(wide.len(), 3);
    assert_eq!(narrow.len(), wide.len());
    const TOLERANCE: f64 = 1e-6;
    for (index, (wide, narrow)) in wide.iter().zip(&narrow).enumerate() {
        assert!(
            (wide - narrow).abs() <= TOLERANCE,
            "node {index}: f64 {wide}, f32 {narrow}, tolerance {TOLERANCE}"
        );
    }
    // And they are not the same numbers: f32 is a narrower computation, not
    // an f64 one rounded at the end. (If this ever fails, the f32 path has
    // stopped being separate and the option has stopped meaning anything.)
    assert!(
        wide.iter().zip(&narrow).any(|(w, n)| w != n),
        "f32 produced bit-identical f64 scores"
    );

    assert!(Registry::drop(name).unwrap());
}

/// Nutmeg dispatches through Grust's own runner rather than a match arm per
/// kernel, so the count below is not a list anyone maintains here. It exists
/// to make the coverage visible: when Grust registers more, this number moves
/// and nothing else has to.
#[test]
fn the_catalog_is_served_whole_and_is_larger_than_the_twelve() {
    let names = algorithm_names();
    assert!(
        names.len() >= 30,
        "Grust registers {} kernels: {names:?}",
        names.len()
    );
    for name in &names {
        let schema = output_schema(name).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(!schema.fields().is_empty(), "{name} produced no columns");
    }
    // And the SQL surface is one function per kernel, plus the three listings
    // (`nutmeg_graphs`, `nutmeg_memory`, `nutmeg_reads`).
    assert_eq!(table_functions().len(), names.len() + 3);
}

/// The README's catalog list is read back out of the file and compared with
/// what is served. The list it replaced ("the twelve") went stale silently,
/// which is the failure this prevents: when Grust registers a kernel, this
/// fails and names it, rather than the page quietly becoming wrong.
#[test]
fn upstream_readme_catalog_matches_reference_algorithms() {
    let readme =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/UPSTREAM_README.md"))
            .expect("the upstream README fixture accompanies the vendored crate");
    let (_, after) = readme
        .split_once("## The catalog")
        .expect("the README has a `## The catalog` section");
    let (section, _) = after.split_once("\n## ").unwrap_or((after, ""));
    let listed: BTreeSet<String> = section
        .split('`')
        .skip(1)
        .step_by(2)
        .filter(|item| {
            // The prose in the section also has backticked names that are not
            // kernels; a kernel is a bare identifier that is actually served.
            item.chars().all(|c| c.is_ascii_alphanumeric()) && resolve_algorithm(item).is_some()
        })
        .map(|item| item.to_string())
        .collect();
    // UPSTREAM_README.md is an unchanged provenance fixture. Nutmeg-local
    // experimental algorithms have their own documentation and catalog test.
    let served: BTreeSet<String> = algorithm_names()
        .iter()
        .filter(|name| !optimized::NAMES.contains(name))
        .map(|n| n.to_string())
        .collect();
    let missing: Vec<&String> = served.difference(&listed).collect();
    assert!(
        missing.is_empty(),
        "the README's catalog list does not name {missing:?}"
    );
    // The stated count is the list's length, so the two cannot drift apart.
    assert!(
        section.contains(&format!("these {} —", served.len())),
        "the README says a different number than the {} kernels served",
        served.len()
    );
    assert_eq!(listed, served);
}

/// The reads of `graph` in [`Registry::reads`].
fn reads_of(graph: &str) -> Vec<ReadInfo> {
    Registry::reads()
        .unwrap()
        .into_iter()
        .filter(|r| r.graph == graph)
        .collect()
}

/// Wait, polling, until `done` holds; fail after a minute.
async fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// A ring of `n` nodes with a chord out of each: every node reaches every
/// other, so a traversal from any source visits all `n` nodes and `2n` arcs.
fn stage_ring(name: &str, n: usize) {
    let id = |i: usize| format!("n{i:06}");
    let (mut source, mut target) = (Vec::with_capacity(2 * n), Vec::with_capacity(2 * n));
    for i in 0..n {
        source.push(id(i));
        target.push(id((i + 1) % n));
        source.push(id(i));
        target.push(id((i * 7_919 + 13) % n));
    }
    let source: Vec<&str> = source.iter().map(String::as_str).collect();
    let target: Vec<&str> = target.iter().map(String::as_str).collect();
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&source, &target, None)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )
    .unwrap();
}

/// Planning a read, and explaining it, run no kernel: the plan is a
/// `NutmegAlgorithmExec`, and neither `execute` nor anything before the
/// stream's first poll starts the read. Polling it runs it once.
#[tokio::test]
async fn planning_and_explaining_a_read_run_no_kernel() -> Result<()> {
    let name = "explained";
    Registry::stage(
        name,
        Part::Edges,
        &[edges(&["a", "b", "c"], &["b", "c", "a"], None)],
        &ColumnMapping::default(),
        true,
        StageOrder::Canonical,
    )?;
    let ctx = SessionContext::new();
    register(&ctx);
    let sql = format!("SELECT * FROM nutmeg_pagerank('{name}')");
    let explained = ctx.sql(&format!("EXPLAIN {sql}")).await?.collect().await?;
    let mut text = String::new();
    for batch in &explained {
        for column in batch.columns() {
            for line in column.as_string::<i32>().iter().flatten() {
                text.push_str(line);
                text.push('\n');
            }
        }
    }
    assert!(
        text.contains("NutmegAlgorithmExec: algorithm=pagerank, graph=explained"),
        "{text}"
    );
    assert!(reads_of(name).is_empty(), "EXPLAIN ran a read");
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    assert!(reads_of(name).is_empty(), "planning ran a read");
    let stream = plan.execute(0, ctx.task_ctx())?;
    assert!(
        reads_of(name).is_empty(),
        "execute() ran a read before it was polled"
    );
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    let reads = reads_of(name);
    assert_eq!(reads.len(), 1, "{reads:?}");
    assert_eq!(reads[0].state, ReadState::Finished, "{reads:?}");
    assert_eq!(reads[0].rows, 3);
    // The same read materialised runs while it is planned, as it always did.
    let materialized = SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new()
            .with_extension(Arc::new(ReadExecution::Materialized)),
    );
    register(&materialized);
    materialized.sql(&sql).await?.create_physical_plan().await?;
    assert_eq!(
        reads_of(name).len(),
        2,
        "a materialised read runs at planning"
    );
    assert!(Registry::drop(name)?);
    Ok(())
}

/// Dropping a read's stream while its kernel runs cancels the read: the
/// kernel stops at its next check with `cancelled`, well short of its work,
/// and the thread returns, releasing everything the read held.
///
/// The kernel is exact betweenness on a ring of 50,000 nodes and 100,000
/// arcs: one traversal per source, each visiting every node and arc, so at
/// least 50,000 x 150,000 = 7.5e9 visits before it can finish. That is over
/// 0.7 s even at an impossible 1e10 visits a second, and the stream is dropped
/// within milliseconds of the kernel's first charge. A kernel the drop did
/// not reach would finish, and its message would then name the consumer
/// rather than the cancellation.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_read_stream_cancels_its_kernel() -> Result<()> {
    const N: usize = 50_000;
    let name = "dropped-stream";
    stage_ring(name, N);
    let table = AlgorithmTable::new("betweenness", name.into(), &no_options())?;
    let exec = AlgorithmExec::try_new(Arc::new(table), None, None)?;
    let mut stream = exec.execute(0, SessionContext::new().task_ctx())?;
    // The first poll starts the read; betweenness has nothing to send yet.
    assert!(
        tokio::time::timeout(Duration::from_millis(1), stream.next())
            .await
            .is_err()
    );
    wait_until("the kernel is charging work", || {
        reads_of(name)
            .first()
            .is_some_and(|r| r.state == ReadState::Running && r.work_units > 0)
    })
    .await;
    drop(stream);
    wait_until("the read has ended", || {
        reads_of(name)
            .first()
            .is_some_and(|r| r.state != ReadState::Running)
    })
    .await;
    let read = reads_of(name).remove(0);
    assert_eq!(read.state, ReadState::Cancelled, "{read:?}");
    let message = read.message.clone().unwrap_or_default();
    assert!(message.contains("cancelled"), "{read:?}");
    assert_eq!(read.rows, 0, "{read:?}");
    assert!(read.work_units < N * 3 * N, "{read:?}");
    assert_eq!(
        read.live_bytes, 0,
        "the read released what it held: {read:?}"
    );
    assert!(Registry::drop(name)?);
    Ok(())
}

/// A streaming read hands its first batches on while the kernel is still
/// producing the rest, and a consumer reading slowly holds it to a few
/// batches: the read's peak is a small fraction of the whole result, which a
/// materialised read holds at once. Both return the same rows in the same
/// order. `allPairsShortestPaths` computes each batch when it is pulled.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_consumer_holds_a_streaming_read_to_a_few_batches() -> Result<()> {
    const N: usize = 1_500;
    let name = "streamed-pairs";
    stage_ring(name, N);
    let table = Arc::new(AlgorithmTable::new(
        "allPairsShortestPaths",
        name.into(),
        &no_options(),
    )?);
    let whole = table.batches()?;
    assert_eq!(whole.iter().map(|b| b.num_rows()).sum::<usize>(), N * N);
    let materialized = reads_of(name).remove(0);
    assert_eq!(materialized.state, ReadState::Finished);

    let exec = AlgorithmExec::try_new(table.clone(), None, None)?;
    let mut stream = exec.execute(0, SessionContext::new().task_ctx())?;
    // Each batch is compared with the materialised one and dropped, as a
    // consumer sending rows on would.
    let mut received = 0;
    let check = |received: &mut usize, batch: RecordBatch| {
        assert_eq!(batch, whole[*received], "batch {received}");
        *received += 1;
    };
    check(&mut received, stream.next().await.expect("a first batch")?);
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let read = reads_of(name).remove(1);
        assert_eq!(read.state, ReadState::Running, "{read:?}");
        assert!(
            read.batches <= received + READ_CHANNEL_BATCHES + 1,
            "the kernel ran ahead of a slow consumer: {read:?}"
        );
        check(&mut received, stream.next().await.expect("more batches")?);
    }
    while let Some(batch) = stream.next().await {
        check(&mut received, batch?);
    }
    assert_eq!(received, whole.len());
    let read = reads_of(name).remove(1);
    assert_eq!(read.state, ReadState::Finished, "{read:?}");
    assert_eq!(read.rows, N * N);
    eprintln!(
        "all pairs on {N} nodes: materialised peak {} bytes, streamed peak {} bytes",
        materialized.peak_bytes, read.peak_bytes
    );
    assert!(
        read.peak_bytes * 20 < materialized.peak_bytes,
        "streamed {read:?}, materialised {materialized:?}"
    );
    drop(whole);
    assert!(Registry::drop(name)?);
    Ok(())
}

/// A `LIMIT` ends the stream once it has its rows, and stops the read.
#[tokio::test(flavor = "multi_thread")]
async fn a_limit_stops_the_read_once_it_has_its_rows() -> Result<()> {
    let name = "limited-stream";
    stage_ring(name, 1_000);
    let ctx = SessionContext::new();
    register(&ctx);
    let batches = ctx
        .sql(&format!(
            "SELECT * FROM nutmeg_all_pairs_shortest_paths('{name}') LIMIT 10"
        ))
        .await?
        .collect()
        .await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
    wait_until("the limited read has ended", || {
        reads_of(name)
            .first()
            .is_some_and(|r| r.state != ReadState::Running)
    })
    .await;
    let read = reads_of(name).remove(0);
    assert_eq!(read.state, ReadState::Cancelled, "{read:?}");
    assert!(read.rows < 1_000 * 1_000, "{read:?}");
    assert!(Registry::drop(name)?);
    Ok(())
}
