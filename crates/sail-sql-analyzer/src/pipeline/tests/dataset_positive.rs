//! Original positive dataset parser fixtures, under both inherited syntaxes.
//! Native TableDefinition replaces Spark TableSpec/ColumnDefinition; configured
//! string storage is not resolved at this SQL-normalization boundary.

use super::*;

fn definition(sql: &str) -> Result<spec::TableDefinition, Box<dyn std::error::Error>> {
    let PipelineStatement::Dataset {
        definition, query, ..
    } = from_ast_pipeline_statement(parse_one_statement(sql)?)?
    else {
        return Err("expected dataset declaration".into());
    };
    assert!(query.is_some());
    Ok(definition)
}

#[test]
fn dataset_column_definitions_preserve_types_nullability_comments_and_defaults() -> TestResult {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        let d = definition(&format!(
            r#"CREATE {kind} table1 (
            a INT NOT NULL,
            b STRUCT<field: STRING, field2: INT> COMMENT "column description comment"
        ) AS SELECT * FROM input"#
        ))?;
        assert_eq!(
            d.columns,
            vec![
                spec::TableColumnDefinition {
                    name: "a".into(),
                    data_type: spec::DataType::Int32,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                },
                spec::TableColumnDefinition {
                    name: "b".into(),
                    data_type: spec::DataType::Struct {
                        fields: [
                            spec::Field {
                                name: "field".into(),
                                data_type: spec::DataType::ConfiguredUtf8 {
                                    utf8_type: spec::Utf8Type::Configured
                                },
                                nullable: true,
                                metadata: vec![]
                            },
                            spec::Field {
                                name: "field2".into(),
                                data_type: spec::DataType::Int32,
                                nullable: true,
                                metadata: vec![]
                            },
                        ]
                        .into_iter()
                        .collect(),
                    },
                    nullable: true,
                    comment: Some("column description comment".into()),
                    default: None,
                    generated_always_as: None,
                    identity: None,
                },
            ]
        );
        assert!(
            definition(&format!("CREATE {kind} table1 AS SELECT * FROM input"))?
                .columns
                .is_empty()
        );
    }
    Ok(())
}

#[test]
fn dataset_comments_preserve_both_present_and_absent_forms() -> TestResult {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        assert_eq!(
            definition(&format!(
                "CREATE {kind} table1 COMMENT 'this is a comment' AS SELECT * FROM input"
            ))?
            .comment
            .as_deref(),
            Some("this is a comment")
        );
        assert_eq!(
            definition(&format!("CREATE {kind} table1 AS SELECT * FROM input"))?.comment,
            None
        );
    }
    Ok(())
}

#[test]
fn dataset_provider_preserves_using_and_default_forms() -> TestResult {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        assert_eq!(
            definition(&format!(
                "CREATE {kind} table1 USING parquet AS SELECT * FROM input"
            ))?
            .file_format,
            Some(spec::TableFileFormat::General {
                format: "parquet".into()
            })
        );
        assert_eq!(
            definition(&format!("CREATE {kind} table1 AS SELECT * FROM input"))?.file_format,
            None
        );
    }
    Ok(())
}

#[test]
fn dataset_create_mode_preserves_if_not_exists() -> TestResult {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        assert_eq!(
            definition(&format!(
                "CREATE {kind} IF NOT EXISTS table1 AS SELECT * FROM input"
            ))?
            .mode,
            spec::CreateTableMode::CreateIfNotExists
        );
        assert_eq!(
            definition(&format!("CREATE {kind} table1 AS SELECT * FROM input"))?.mode,
            spec::CreateTableMode::Create
        );
    }
    Ok(())
}

#[test]
fn dataset_properties_preserve_boolean_string_and_numeric_values() -> TestResult {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        let properties = definition(&format!(
            "CREATE {kind} table1 TBLPROPERTIES(a=FALSE, b='long comment', c=100) AS SELECT 1"
        ))?
        .properties;
        assert_eq!(
            properties
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>(),
            std::collections::BTreeMap::from([
                ("a".into(), "false".into()),
                ("b".into(), "long comment".into()),
                ("c".into(), "100".into())
            ])
        );
        assert!(
            definition(&format!("CREATE {kind} tabel1 AS SELECT 1"))?
                .properties
                .is_empty()
        );
    }
    Ok(())
}

#[test]
fn dataset_partitioning_preserves_identity_column_and_empty_forms() -> TestResult {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        assert_eq!(
            definition(&format!(
                "CREATE {kind} table1 PARTITIONED BY (a) AS SELECT * FROM input"
            ))?
            .partition_by,
            vec![spec::PartitionColumn::Expression(
                spec::Expr::UnresolvedAttribute {
                    name: vec!["a".to_owned()].into(),
                    plan_id: None,
                    is_metadata_column: false
                }
            )]
        );
        assert!(
            definition(&format!("CREATE {kind} table1 AS SELECT * FROM input"))?
                .partition_by
                .is_empty()
        );
    }
    Ok(())
}
