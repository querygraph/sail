use std::collections::{BTreeMap, BTreeSet, HashMap};

use sail_common::spec;
use sail_sql_analyzer::expression::from_ast_object_name;
use sail_sql_analyzer::parser::parse_object_name;
use tokio::sync::watch;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::{self as sc, pipeline_command as pc};

pub(super) type Name = Vec<String>;

pub(super) fn parse_name(name: &str) -> SparkResult<Name> {
    let name: Vec<String> = from_ast_object_name(parse_object_name(name)?)?.into();
    Ok(name.into_iter().map(|x| x.to_ascii_lowercase()).collect())
}

pub(super) fn quoted(name: &[String]) -> String {
    name.iter()
        .map(|x| format!("`{}`", x.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn identifier(name: &[String]) -> sc::ResolvedIdentifier {
    match name {
        [catalog, namespace, table] => sc::ResolvedIdentifier {
            catalog_name: catalog.clone(),
            namespace: vec![namespace.clone()],
            table_name: table.clone(),
        },
        [namespace, table] => sc::ResolvedIdentifier {
            namespace: vec![namespace.clone()],
            table_name: table.clone(),
            ..Default::default()
        },
        [table] => sc::ResolvedIdentifier {
            table_name: table.clone(),
            ..Default::default()
        },
        _ => sc::ResolvedIdentifier::default(),
    }
}

pub(super) fn analysis(condition: &str, message: impl std::fmt::Display) -> SparkError {
    SparkError::AnalysisError(format!("[{condition}] {message}"))
}

#[derive(Clone)]
pub(super) struct Output {
    pub name: Name,
    pub kind: sc::OutputType,
    pub definition: pc::DefineOutput,
    pub schema: Option<spec::Schema>,
    pub view: Option<spec::ViewDefinition>,
}

#[derive(Clone)]
pub(super) struct Flow {
    pub name: Name,
    pub target: Name,
    pub query: spec::QueryPlan,
    pub sql_conf: HashMap<String, String>,
    pub catalog: String,
    pub database: String,
}

#[derive(Clone)]
pub(super) struct GraphDefinition {
    pub catalog: String,
    pub database: String,
    pub sql_conf: HashMap<String, String>,
    pub outputs: Vec<Output>,
    pub flows: Vec<Flow>,
}

pub(crate) struct PipelineGraph {
    pub(super) definition: GraphDefinition,
    active: Option<watch::Sender<bool>>,
}

impl PipelineGraph {
    pub(super) fn new(
        catalog: String,
        database: String,
        sql_conf: HashMap<String, String>,
    ) -> Self {
        Self {
            definition: GraphDefinition {
                catalog,
                database,
                sql_conf,
                outputs: vec![],
                flows: vec![],
            },
            active: None,
        }
    }

    pub(super) fn ensure_idle(&self) -> SparkResult<()> {
        if self.active.as_ref().is_some_and(|x| x.receiver_count() > 0) {
            return Err(SparkError::invalid("a pipeline run is already active"));
        }
        Ok(())
    }

    pub(crate) fn cancel(&mut self) {
        if let Some(signal) = &self.active {
            let _ = signal.send(true);
        }
    }

    pub(super) fn start(&mut self) -> SparkResult<(GraphDefinition, watch::Receiver<bool>)> {
        self.ensure_idle()?;
        let (tx, rx) = watch::channel(false);
        self.active = Some(tx);
        Ok((self.definition.clone(), rx))
    }

    pub(super) fn define_output(&mut self, definition: pc::DefineOutput) -> SparkResult<Name> {
        self.ensure_idle()?;
        let kind = sc::OutputType::try_from(definition.output_type.required("output type")?)?;
        let raw = parse_name(definition.output_name.as_deref().required("output name")?)?;
        let name = match kind {
            sc::OutputType::MaterializedView | sc::OutputType::Table => {
                self.definition.qualify(&raw)?
            }
            sc::OutputType::TemporaryView | sc::OutputType::Sink => {
                if raw.len() != 1 {
                    return Err(analysis("TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS", quoted(&raw)));
                }
                raw
            }
            sc::OutputType::Unspecified => {
                return Err(SparkError::invalid("unspecified pipeline output type"));
            }
        };
        self.definition.outputs.push(Output {
            name: name.clone(),
            kind,
            schema: super::schema::declared_schema(&definition)?,
            view: None,
            definition,
        });
        Ok(name)
    }

    pub(super) fn define_flow(&mut self, definition: pc::DefineFlow) -> SparkResult<Name> {
        self.ensure_idle()?;
        let raw_name = definition.flow_name.as_deref().required("flow name")?;
        if definition.once.is_some() {
            return Err(analysis("DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED", raw_name));
        }
        let raw_target = definition
            .target_dataset_name
            .as_deref()
            .required("target dataset name")?;
        let name = parse_name(raw_name)?;
        if raw_name != raw_target && name.len() != 1 {
            return Err(analysis("MULTIPART_FLOW_NAME_NOT_SUPPORTED", raw_name));
        }
        let target = parse_name(raw_target)?;
        let private = self.definition.outputs.iter().any(|x| {
            x.name == target
                && matches!(x.kind, sc::OutputType::TemporaryView | sc::OutputType::Sink)
        });
        let (name, target) = if private {
            (name, target)
        } else {
            (
                self.definition.qualify(&name)?,
                self.definition.qualify(&target)?,
            )
        };
        let query = match definition.details.required("flow details")? {
            pc::define_flow::Details::RelationFlowDetails(details) => {
                let plan: spec::Plan = details.relation.required("flow relation")?.try_into()?;
                match plan {
                    spec::Plan::Query(query) => query,
                    spec::Plan::Command(_) => {
                        return Err(analysis(
                            "UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND",
                            "flow must return a query",
                        ));
                    }
                }
            }
            _ => return Err(SparkError::unsupported("pipeline Auto CDC flow")),
        };
        let mut sql_conf = self.definition.sql_conf.clone();
        sql_conf.extend(definition.sql_conf);
        self.definition.flows.push(Flow {
            name: name.clone(),
            target,
            query,
            sql_conf,
            catalog: self.definition.catalog.clone(),
            database: self.definition.database.clone(),
        });
        Ok(name)
    }
}

impl GraphDefinition {
    pub(super) fn qualify(&self, name: &[String]) -> SparkResult<Name> {
        match name {
            [table] => Ok(vec![
                self.catalog.clone(),
                self.database.clone(),
                table.clone(),
            ]),
            [database, table] => Ok(vec![self.catalog.clone(), database.clone(), table.clone()]),
            [_, _, _] => Ok(name.to_vec()),
            _ => Err(analysis("REQUIRES_SINGLE_PART_NAMESPACE", quoted(name))),
        }
    }

    pub(super) fn resolve_name(&self, name: &[String]) -> SparkResult<Name> {
        let name: Name = name.iter().map(|x| x.to_ascii_lowercase()).collect();
        if self.outputs.iter().any(|x| x.name == name) {
            Ok(name)
        } else {
            self.qualify(&name)
        }
    }

    pub(super) fn validate(&self) -> SparkResult<()> {
        if !self
            .outputs
            .iter()
            .any(|x| x.kind != sc::OutputType::TemporaryView)
        {
            return Err(analysis("RUN_EMPTY_PIPELINE", "no materialized outputs"));
        }
        let mut names = BTreeSet::new();
        for output in &self.outputs {
            if !names.insert(&output.name) {
                return Err(analysis(
                    "PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT",
                    quoted(&output.name),
                ));
            }
        }
        let mut flows = BTreeSet::new();
        let mut producers: BTreeMap<&Name, Vec<&Flow>> = BTreeMap::new();
        for flow in &self.flows {
            if !flows.insert(&flow.name) {
                return Err(analysis(
                    "PIPELINE_DUPLICATE_IDENTIFIERS.FLOW",
                    quoted(&flow.name),
                ));
            }
            if flow.name != flow.target && names.contains(&flow.name) {
                return Err(analysis(
                    "FLOW_NAME_CONFLICTS_WITH_TABLE",
                    quoted(&flow.name),
                ));
            }
            if !names.contains(&flow.target) {
                return Err(analysis("FLOW_TARGET_NOT_DEFINED", quoted(&flow.target)));
            }
            producers.entry(&flow.target).or_default().push(flow);
        }
        for output in &self.outputs {
            let Some(flows) = producers.get(&output.name) else {
                return Err(analysis(
                    "PIPELINE_DATASET_WITHOUT_FLOW",
                    quoted(&output.name),
                ));
            };
            if output.kind == sc::OutputType::MaterializedView && flows.len() != 1 {
                return Err(analysis(
                    "MATERIALIZED_VIEW_WITH_MULTIPLE_QUERIES",
                    quoted(&output.name),
                ));
            }
            for flow in flows {
                let streaming = crate::service::is_streaming_query_plan(&flow.query);
                match output.kind {
                    sc::OutputType::Table if !streaming => {
                        return Err(analysis(
                            "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE",
                            quoted(&flow.name),
                        ));
                    }
                    sc::OutputType::MaterializedView if streaming => {
                        return Err(analysis(
                            if output.view.is_some() {
                                "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_PERSISTED_VIEW"
                            } else {
                                "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW"
                            },
                            quoted(&flow.name),
                        ));
                    }
                    sc::OutputType::Sink if !streaming => {
                        return Err(analysis(
                            "INVALID_FLOW_QUERY_TYPE",
                            "sink requires a streaming flow",
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}
