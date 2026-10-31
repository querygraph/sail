//! Replay of the JVM oracle in `lookup_oracle.json`: `StructType.getFieldIndex`
//! and `getFieldIndexCaseInsensitive` on the pinned Spark 4.2.1 jars for
//! designed and seeded schemas (four/five-distinct boundary, raw length above
//! four with four spellings, duplicate updates around the transition, shared
//! hash buckets and deeper prefixes, unrelated-name placement changes, true
//! full-hash collisions, case variants and permutations, empty and
//! supplementary names, the pinned ROOT mismatch pairs).

use serde::Deserialize;

use super::*;
use crate::pipelines::autocdc::lookup::{FieldLookup, improved_hash};

#[derive(Deserialize)]
struct Oracle {
    #[serde(default)]
    spark_version: String,
    #[serde(default)]
    spark_revision: String,
    cases: Vec<OracleCase>,
}

#[derive(Deserialize)]
struct OracleCase {
    fields: Vec<String>,
    queries: Vec<Query>,
}

#[derive(Deserialize)]
struct Query {
    name: String,
    exact: i64,
    insensitive: i64,
}

fn oracle() -> Oracle {
    #[expect(clippy::expect_used)]
    serde_json::from_str(include_str!("../lookup_oracle.json"))
        .expect("the JVM oracle fixture should parse")
}

fn expected(index: i64) -> Option<usize> {
    usize::try_from(index).ok()
}

#[test]
fn every_oracle_query_resolves_as_on_the_pinned_spark_jars() {
    let oracle = oracle();
    assert_eq!(oracle.spark_version, "4.2.1");
    assert_eq!(
        oracle.spark_revision,
        "617618396087429b5627a86ce933bb0f5d6645cb"
    );
    assert!(oracle.cases.len() >= 500);
    let mut queries = 0;
    for case in &oracle.cases {
        let lookup = FieldLookup::new(&case.fields);
        let exact = lookup.resolver(Case::Sensitive);
        let insensitive = lookup.resolver(Case::Insensitive);
        for query in &case.queries {
            queries += 1;
            assert_eq!(
                exact(&query.name),
                expected(query.exact),
                "exact lookup of {:?} in {:?}",
                query.name,
                case.fields
            );
            assert_eq!(
                insensitive(&query.name),
                expected(query.insensitive),
                "case-insensitive lookup of {:?} in {:?}",
                query.name,
                case.fields
            );
        }
    }
    assert!(queries >= 6000);
}

#[test]
fn improved_java_string_hashes_match_scala() {
    // "Aa" and "BB" share Java's String.hashCode (2112); the improved hash
    // keeps them equal, which is what makes them a collision leaf.
    assert_eq!(improved_hash("Aa"), improved_hash("BB"));
    assert_ne!(improved_hash("Aa"), improved_hash("aa"));
    // Values computed independently with Python integer arithmetic from the
    // bytecode-derived formula in the design note.
    assert_eq!(improved_hash("a"), 0xfff7_296a);
    assert_eq!(improved_hash(""), 0xff83_ef00);
    // Supplementary characters hash as two UTF-16 units.
    assert_eq!(improved_hash("\u{10400}"), 0x6b2a_1f91);
}

#[test]
fn selection_through_the_resolver_follows_the_map_policy() {
    // From the oracle: insensitive `a` is index 1 in [a, A, b, c] and index 0
    // once an unrelated `d` pushes the map to its hash representation.
    for (fields, include, expected_indices) in [
        (names(&["a", "A", "b", "c"]), "a", vec![1]),
        (names(&["a", "A", "b", "c", "d"]), "a", vec![0]),
        (names(&["a", "A", "a"]), "A", vec![1]),
        (names(&["b", "c", "d", "a", "A"]), "A", vec![3]),
    ] {
        let lookup = FieldLookup::new(&fields);
        let selection = grust_columns::Selection::Include(&[include]);
        assert_eq!(
            grust_columns::select(fields.len(), selection, lookup.resolver(Case::Insensitive)),
            Ok(expected_indices),
            "{fields:?}"
        );
    }
    let fields = names(&["a", "A", "a"]);
    let lookup = FieldLookup::new(&fields);
    assert_eq!(
        grust_columns::select(
            3,
            grust_columns::Selection::Exclude(&["a"]),
            lookup.resolver(Case::Sensitive)
        ),
        Ok(vec![0, 1])
    );
}

/// Opt-in replay of Codex's independent corpus (Grust evidence
/// fable-c4-independent-review: 256 fresh schemas, 16,684 queries, too large
/// to embed). Set SAIL_FABLE_LOOKUP_CORPUS to the compact JSON path; the
/// replay result is recorded in the Fable evidence packet.
#[test]
fn optional_review_corpus_replays_against_the_adapter() {
    let Ok(path) = std::env::var("SAIL_FABLE_LOOKUP_CORPUS") else {
        eprintln!("SAIL_FABLE_LOOKUP_CORPUS unset; review corpus replay skipped");
        return;
    };
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let Ok(corpus) = serde_json::from_str::<Oracle>(&text) else {
        eprintln!("could not parse {path}");
        return;
    };
    let mut queries = 0;
    for case in &corpus.cases {
        let lookup = FieldLookup::new(&case.fields);
        let exact = lookup.resolver(Case::Sensitive);
        let insensitive = lookup.resolver(Case::Insensitive);
        for query in &case.queries {
            queries += 1;
            assert_eq!(
                exact(&query.name),
                expected(query.exact),
                "exact {:?} in {:?}",
                query.name,
                case.fields
            );
            assert_eq!(
                insensitive(&query.name),
                expected(query.insensitive),
                "insensitive {:?} in {:?}",
                query.name,
                case.fields
            );
        }
    }
    eprintln!(
        "review corpus: {} schemas, {queries} queries replayed",
        corpus.cases.len()
    );
    assert!(queries > 0);
}
