//! Fixtures from Spark 4.2.1's own suites (`ConnectValidPipelineSuite`,
//! `ConnectInvalidPipelineSuite`, source `617618396`) and from the runtime
//! observation packets `flow-config-fusion-reference` and
//! `flow-config-cte-reference`, reduced to their map inputs.

use super::*;

fn conf(entries: &[(&str, &str)]) -> ConfMap {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn view<'a>(flow_name: &'a str, accepted: &'a ConfMap) -> ViewContribution<'a> {
    ViewContribution {
        flow_name,
        accepted,
    }
}

#[test]
fn a_consumer_fuses_its_declared_conf_with_every_used_view() -> Result<(), DuplicateFlowSqlConf> {
    // ConnectValidPipelineSuite "Connect retains and fuses confs" (:264–288):
    // d reads b (which read a) and c; d's accepted conf is the union of all four.
    let a = conf(&[("a", "a-val")]);
    let b = conf(&[("a", "a-val"), ("b", "b-val")]); // b's accepted map already includes a's
    let c = conf(&[("c", "c-val")]);
    let declared = conf(&[("d", "d-val")]);
    let accepted = merge("d", "d", &declared, [view("b", &b), view("c", &c)])?;
    assert_eq!(
        accepted.map,
        conf(&[
            ("a", "a-val"),
            ("b", "b-val"),
            ("c", "c-val"),
            ("d", "d-val")
        ])
    );
    assert!(accepted.changed);
    // Transitively: b itself merged a's conf when it resolved.
    let b_accepted = merge("b", "b", &conf(&[("b", "b-val")]), [view("a", &a)])?;
    assert_eq!(b_accepted.map, b);
    Ok(())
}

#[test]
fn a_table_input_contributes_nothing_so_the_declared_conf_is_unchanged()
-> Result<(), DuplicateFlowSqlConf> {
    // "Confs aren't fused past materialization points" (:289–321): the caller
    // passes no contribution for a table, so c keeps only its own conf.
    let declared = conf(&[("c", "c-val")]);
    let accepted = merge("c", "c", &declared, [])?;
    assert_eq!(accepted.map, declared);
    assert!(!accepted.changed);
    Ok(())
}

#[test]
fn identical_values_from_several_flows_are_not_a_conflict() -> Result<(), DuplicateFlowSqlConf> {
    // "Setting the same conf with the same value is totally cool" (:322–345).
    let a = conf(&[("key", "val")]);
    let b = conf(&[("key", "val")]);
    let declared = conf(&[("key", "val")]);
    let accepted = merge("c", "c", &declared, [view("a", &a), view("b", &b)])?;
    assert_eq!(accepted.map, conf(&[("key", "val")]));
    assert!(!accepted.changed);
    Ok(())
}

#[test]
fn a_view_table_conflict_reports_sparks_parameters_sorted_by_value() {
    // ConnectInvalidPipelineSuite "view-table conf conflict" (:350–373): view a
    // sets x=a-val, table flow b sets x=b-val and reads a.
    let a = conf(&[("x", "a-val")]);
    let declared = conf(&[("x", "b-val")]);
    let error = merge("b", "b", &declared, [view("a", &a)]).err();
    assert_eq!(
        error,
        Some(DuplicateFlowSqlConf {
            key: "x".into(),
            dataset_name: "b".into(),
            flow_name1: "a".into(),
            flow_name2: "b".into(),
        })
    );
}

#[test]
fn a_view_view_conflict_names_the_two_views_not_the_consumer() {
    // "view-view conf conflict" (:374–): the consumer declares nothing for x.
    let a = conf(&[("x", "a-val")]);
    let b = conf(&[("x", "b-val")]);
    let error = merge("c", "c", &ConfMap::new(), [view("a", &a), view("b", &b)]).err();
    assert_eq!(
        error,
        Some(DuplicateFlowSqlConf {
            key: "x".into(),
            dataset_name: "c".into(),
            flow_name1: "a".into(),
            flow_name2: "b".into(),
        })
    );
}

#[test]
fn the_message_follows_sparks_template() {
    let error = DuplicateFlowSqlConf {
        key: "spark.sql.caseSensitive".into(),
        dataset_name: "spark_catalog.default.target".into(),
        flow_name1: "spark_catalog.default.a_insensitive".into(),
        flow_name2: "spark_catalog.default.z_sensitive".into(),
    };
    assert_eq!(
        error.to_string(),
        "Found duplicate sql conf for dataset 'spark_catalog.default.target': \
         'spark.sql.caseSensitive' is defined by both 'spark_catalog.default.a_insensitive' \
         and 'spark_catalog.default.z_sensitive'"
    );
}

#[test]
fn inherited_keys_are_attributed_to_the_immediate_view() {
    // b's accepted map carries x inherited from a; a consumer that sets x
    // differently conflicts with b, the immediate contributor, not with a.
    let b = conf(&[("x", "from-a"), ("b", "b-val")]);
    let declared = conf(&[("x", "mine")]);
    let error = merge("c", "c", &declared, [view("b", &b)]).err();
    assert_eq!(
        error.map(|e| (e.key, e.flow_name1, e.flow_name2)),
        Some(("x".into(), "b".into(), "c".into()))
    );
}

#[test]
fn ties_in_value_keep_contribution_order_for_the_reported_names() {
    // Three contributors: two share the smaller value. Stable sort by value
    // makes the first of them `flowName1`; the odd one out is `flowName2`.
    let a = conf(&[("x", "1")]);
    let b = conf(&[("x", "1")]);
    let declared = conf(&[("x", "2")]);
    let error = merge("c", "c", &declared, [view("a", &a), view("b", &b)]).err();
    assert_eq!(
        error.map(|e| (e.flow_name1, e.flow_name2)),
        Some(("a".into(), "c".into()))
    );
}

#[test]
fn several_conflicting_keys_report_one_of_them_with_consistent_names() {
    // Spark searches a grouped map and guarantees no key order; this
    // implementation reports the smallest key. Either way the names must be
    // the contributors of the reported key.
    let a = conf(&[("x", "a"), ("y", "a")]);
    let declared = conf(&[("x", "c"), ("y", "c")]);
    let error = merge("c", "c", &declared, [view("a", &a)]).err();
    assert!(
        matches!(
            &error,
            Some(e) if ["x", "y"].contains(&e.key.as_str()) && e.flow_name1 == "a" && e.flow_name2 == "c"
        ),
        "{error:?}"
    );
}

#[test]
fn identical_repeated_contributions_are_harmless() -> Result<(), DuplicateFlowSqlConf> {
    // The caller supplies distinct inputs by canonical identity; if the same
    // view arrives twice with the same accepted map, equal values never
    // conflict and the union is unchanged.
    let a = conf(&[("x", "a-val")]);
    let accepted = merge("c", "c", &ConfMap::new(), [view("a", &a), view("a", &a)])?;
    assert_eq!(accepted.map, a);
    assert!(accepted.changed);
    Ok(())
}

#[test]
fn equal_display_names_with_different_values_still_conflict() {
    // Two distinct inputs whose display strings collide (quoted multipart
    // identifiers containing dots) are both kept: the conflict is reported
    // and the two flow-name parameters are legitimately equal.
    let first = conf(&[("x", "1")]);
    let second = conf(&[("x", "2")]);
    let error = merge(
        "c",
        "c",
        &ConfMap::new(),
        [view("db.v", &first), view("db.v", &second)],
    )
    .err();
    assert_eq!(
        error,
        Some(DuplicateFlowSqlConf {
            key: "x".into(),
            dataset_name: "c".into(),
            flow_name1: "db.v".into(),
            flow_name2: "db.v".into(),
        })
    );
}

#[test]
fn the_first_pass_map_is_overwritten_in_binding_order_with_repeats() {
    // flow-config-fusion-reference attempt-02: z_sensitive declares
    // caseSensitive=true, a_insensitive declares false. Reading z then a leaves
    // false for the first pass; reading a then z leaves true. A repeated read
    // of the same view rewrites the same value.
    let z = conf(&[("spark.sql.caseSensitive", "true")]);
    let a = conf(&[("spark.sql.caseSensitive", "false")]);
    let declared = ConfMap::new();
    let z_then_a = first_pass_map(&declared, [view("z", &z), view("a", &a)]);
    assert_eq!(z_then_a, conf(&[("spark.sql.caseSensitive", "false")]));
    let a_then_z = first_pass_map(&declared, [view("a", &a), view("z", &z)]);
    assert_eq!(a_then_z, conf(&[("spark.sql.caseSensitive", "true")]));
    let repeated = first_pass_map(&declared, [view("z", &z), view("a", &a), view("z", &z)]);
    assert_eq!(repeated, conf(&[("spark.sql.caseSensitive", "true")]));
}

#[test]
fn the_declared_conf_is_the_base_of_the_first_pass_and_views_overwrite_it() {
    // A consumer declaring true that reads a false view analyzes its first
    // pass under false (then the merge rejects the conflict).
    let a = conf(&[("spark.sql.caseSensitive", "false")]);
    let declared = conf(&[("spark.sql.caseSensitive", "true"), ("other", "kept")]);
    let first = first_pass_map(&declared, [view("a", &a)]);
    assert_eq!(
        first,
        conf(&[("spark.sql.caseSensitive", "false"), ("other", "kept")])
    );
    assert!(merge("c", "c", &declared, [view("a", &a)]).is_err());
}

#[test]
fn an_unused_cte_definition_still_contributes_in_the_first_pass_and_the_merge() {
    // flow-config-cte-reference `cte_unused_definition`: the CTE's view (true)
    // is bound before the body's view (false) although the CTE is unused; the
    // first pass runs under false and the merge is a conflict.
    let staged = conf(&[("spark.sql.caseSensitive", "true")]);
    let body = conf(&[("spark.sql.caseSensitive", "false")]);
    let first = first_pass_map(
        &ConfMap::new(),
        [view("z_sensitive", &staged), view("a_insensitive", &body)],
    );
    assert_eq!(first, conf(&[("spark.sql.caseSensitive", "false")]));
    let error = merge(
        "target",
        "target",
        &ConfMap::new(),
        [view("z_sensitive", &staged), view("a_insensitive", &body)],
    )
    .err();
    assert_eq!(
        error.map(|e| (e.flow_name1, e.flow_name2)),
        Some(("a_insensitive".into(), "z_sensitive".into()))
    );
}

#[test]
fn keys_and_values_are_not_normalized() -> Result<(), DuplicateFlowSqlConf> {
    // Differently cased keys are distinct keys; differently cased values are
    // distinct values. Spark compares raw strings.
    let a = conf(&[("Spark.Sql.X", "TRUE")]);
    let declared = conf(&[("spark.sql.x", "true")]);
    let accepted = merge("c", "c", &declared, [view("a", &a)])?;
    assert_eq!(accepted.map.len(), 2);
    let b = conf(&[("spark.sql.x", "TRUE")]);
    assert!(merge("c", "c", &declared, [view("b", &b)]).is_err());
    Ok(())
}
