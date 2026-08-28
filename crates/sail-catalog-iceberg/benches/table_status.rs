#![expect(
    clippy::expect_used,
    reason = "benchmark fixtures must fail immediately when their schema is invalid"
)]

use std::hint::black_box;
use std::time::Instant;

use sail_catalog::provider::Namespace;
use sail_catalog_iceberg::load_table_result_to_status;
use sail_catalog_iceberg::models::LoadTableResult;
use serde_json::json;

const SAMPLES: usize = 9;

fn selected(name: &str) -> bool {
    std::env::var("SAIL_BENCH_FILTER")
        .map(|filter| name.contains(&filter))
        .unwrap_or(true)
}

fn measure_batched<T: Clone>(name: &str, iterations: usize, input: &T, mut run: impl FnMut(T)) {
    if !selected(name) {
        return;
    }

    for _ in 0..(iterations / 20).max(1) {
        run(input.clone());
    }

    let mut nanos = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let inputs = (0..iterations).map(|_| input.clone()).collect::<Vec<_>>();
        let started = Instant::now();
        for input in inputs {
            run(input);
        }
        nanos.push(started.elapsed().as_nanos() as f64 / iterations as f64);
    }
    nanos.sort_by(f64::total_cmp);
    println!(
        "{name}\titerations={iterations}\tmin_ns={:.1}\tmedian_ns={:.1}\tmax_ns={:.1}",
        nanos[0],
        nanos[SAMPLES / 2],
        nanos[SAMPLES - 1]
    );
}

fn load_result(column_count: usize, with_relationships: bool) -> LoadTableResult {
    let fields = (0..column_count)
        .map(|index| {
            json!({
                "id": index + 1,
                "name": format!("column_{index}"),
                "required": index % 3 == 0,
                "type": if index % 5 == 0 { "long" } else { "string" },
                "doc": format!("benchmark column {index}"),
            })
        })
        .collect::<Vec<_>>();
    let relationship_ids = if with_relationships {
        (1..=column_count).step_by(8).collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let partition_fields = relationship_ids
        .iter()
        .enumerate()
        .map(|(index, field_id)| {
            json!({
                "source-id": field_id,
                "field-id": 1000 + index,
                "name": format!("partition_{field_id}"),
                "transform": if index % 2 == 0 { "identity" } else { "bucket[16]" },
            })
        })
        .collect::<Vec<_>>();
    let sort_fields = relationship_ids
        .iter()
        .map(|field_id| {
            json!({
                "source-id": field_id,
                "transform": "identity",
                "direction": if field_id % 2 == 0 { "desc" } else { "asc" },
                "null-order": "nulls-first",
            })
        })
        .collect::<Vec<_>>();
    let identifier_ids = relationship_ids.iter().take(4).copied().collect::<Vec<_>>();

    serde_json::from_value(json!({
        "metadata-location": "s3://warehouse/events/metadata/v1.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "11111111-1111-1111-1111-111111111111",
            "location": "s3://warehouse/events",
            "last-updated-ms": 1_710_000_000_000_i64,
            "properties": {
                "comment": "benchmark table",
                "owner": "querygraph",
            },
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": fields,
                "identifier-field-ids": identifier_ids,
            }],
            "current-schema-id": 0,
            "last-column-id": column_count,
            "partition-specs": [{
                "spec-id": 0,
                "fields": partition_fields,
            }],
            "default-spec-id": 0,
            "last-partition-id": 1000 + relationship_ids.len(),
            "sort-orders": [{
                "order-id": 0,
                "fields": sort_fields,
            }],
            "default-sort-order-id": 0,
            "current-snapshot-id": 42,
            "last-sequence-number": 7,
        },
    }))
    .expect("valid Iceberg REST load-table fixture")
}

fn benchmark_case(column_count: usize, with_relationships: bool, iterations: usize) {
    let name = format!(
        "table_status/{}/{}",
        if with_relationships {
            "relationships"
        } else {
            "plain"
        },
        column_count
    );
    let input = load_result(column_count, with_relationships);
    let database =
        Namespace::try_from(vec!["analytics".to_string()]).expect("valid benchmark namespace");
    measure_batched(&name, iterations, &input, |input| {
        black_box(load_table_result_to_status(
            black_box("benchmark"),
            black_box("events"),
            black_box(&database),
            black_box(input),
        ))
        .expect("valid table status");
    });
}

fn main() {
    for (columns, iterations) in [(32, 1_500), (256, 300), (1_024, 60)] {
        benchmark_case(columns, false, iterations);
        benchmark_case(columns, true, iterations);
    }
}
