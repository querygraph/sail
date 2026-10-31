use std::collections::HashMap;
use std::sync::Mutex;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use sail_plan::config::PlanConfig;
use sail_plan::error::{PlanError, PlanResult};
use sail_plan::resolver::PlanResolver;

use super::*;
use crate::pipelines::graph::Output;
use crate::spark::connect::OutputType;

fn read(table: &str, streaming: bool) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: spec::ObjectName::bare(table),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: streaming,
    })
}

fn samples(method: spec::TableSampleMethod) -> Vec<spec::QueryPlan> {
    let sample = spec::TableSample {
        method,
        seed: Some(1),
    };
    let mut named = read("events", false);
    if let spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(table),
        ..
    } = &mut named.node
    {
        table.sample = Some(sample.clone());
    }
    vec![
        named,
        spec::QueryPlan::new(spec::QueryNode::TableSample {
            input: Box::new(read("events", false)),
            sample,
        }),
    ]
}

fn scalar(query: spec::QueryPlan) -> spec::Expr {
    spec::Expr::ScalarSubquery {
        subquery: Box::new(query),
    }
}

fn classify(plan: &spec::QueryPlan) -> Result<ReadSet, BindingError> {
    let graph = GraphDefinition {
        catalog: "c".into(),
        database: "d".into(),
        sql_conf: HashMap::new(),
        outputs: vec![Output {
            name: vec!["c".into(), "d".into(), "events".into()],
            kind: OutputType::Table,
            definition: Default::default(),
            schema: None,
            view: None,
        }],
        flows: vec![],
    };
    let flow = Flow {
        name: vec!["result".into()],
        target: vec!["result".into()],
        query: plan.clone(),
        sql_conf: HashMap::new(),
        catalog: "c".into(),
        database: "d".into(),
        auto_cdc: None,
    };
    validate_reads(&SessionContext::new(), &graph, &flow, plan, &[], &[], &[])
}

#[test]
fn both_sample_sites_classify_hidden_reads_and_helpers() {
    for streaming in [false, true] {
        let value = scalar(read("hidden", streaming));
        for method in [
            spec::TableSampleMethod::Percent {
                value: value.clone(),
            },
            spec::TableSampleMethod::Rows { value },
        ] {
            for plan in samples(method) {
                let name = vec!["c".into(), "d".into(), "hidden".into()];
                let expected = if streaming {
                    BindingError::UnboundStreamingRead(name)
                } else {
                    BindingError::UnboundCatalogRead(name)
                };
                assert_eq!(classify(&plan), Err(expected));
            }
        }
    }
    let tail = spec::QueryPlan::new(spec::QueryNode::Tail {
        input: Box::new(read("events", false)),
        limit: spec::Expr::Literal(spec::Literal::Int32 { value: Some(1) }),
    });
    for plan in samples(spec::TableSampleMethod::Percent {
        value: scalar(tail),
    }) {
        assert_eq!(classify(&plan), Err(BindingError::Tail));
    }
    for method in [
        spec::TableSampleMethod::Percent {
            value: spec::Expr::Literal(spec::Literal::Int32 { value: Some(50) }),
        },
        spec::TableSampleMethod::Bucket {
            numerator: 1,
            denominator: 2,
        },
    ] {
        for plan in samples(method) {
            assert_eq!(
                classify(&plan).map(|reads| reads.local),
                Ok(BTreeSet::from([vec![
                    "c".into(),
                    "d".into(),
                    "events".into()
                ]]))
            );
        }
    }
}

/// Percent resolves its expression before rejecting a non-foldable subquery.
/// This witnesses the hidden planning read, not successful subquery sampling.
/// Native ROWS sampling is currently unsupported; it is not an I/O witness.
#[tokio::test]
async fn native_percent_sampling_reaches_hidden_reads_before_validation()
-> Result<(), Box<dyn std::error::Error>> {
    struct Scope {
        reads: Mutex<Vec<Vec<String>>>,
        plan: LogicalPlan,
    }
    impl RelationScope for Scope {
        fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
            self.reads
                .lock()
                .map_err(|e| PlanError::internal(e.to_string()))?
                .push(name.to_vec());
            if name == ["hidden"] {
                Err(PlanError::analysis("sample expression reached hidden read"))
            } else {
                Ok(Some(self.plan.clone()))
            }
        }
    }
    let plan = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]))?),
    });
    for query in samples(spec::TableSampleMethod::Percent {
        value: scalar(read("hidden", false)),
    }) {
        let scope = Scope {
            reads: Mutex::default(),
            plan: plan.clone(),
        };
        let ctx = SessionContext::new();
        let result = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()))
            .with_relation_scope(&scope)
            .resolve_named_plan(spec::Plan::Query(query))
            .await;
        assert!(result.is_err_and(|e| {
            e.to_string()
                .contains("sample expression reached hidden read")
        }));
        assert_eq!(
            scope.reads.into_inner()?,
            vec![vec!["events".to_string()], vec!["hidden".to_string()]]
        );
    }
    Ok(())
}
