//! Limits on eager work during pipeline definition, in the order Spark applies them.
//!
//! PySpark marks requests with a `PipelineAnalysisContext` user-context extension.
//! Any such request is a pipeline definition, and one naming a flow comes from inside
//! a query function, which may not run or analyze anything eagerly. A flow name alone
//! decides that, as in `PipelineAnalysisContextUtils.isInsidePipelineFlowFunction`.
use std::collections::HashMap;

use prost::Message;
use sail_common::spec;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::pipeline::{PipelineStatement, from_ast_pipeline_statement};

use crate::error::{SparkError, SparkResult, StructuredSparkError};
use crate::spark::connect::{
    PipelineAnalysisContext, Relation, UserContext, command, plan, relation,
};

const CONTEXT_TYPE: &str = "spark.connect.PipelineAnalysisContext";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PipelineRequest {
    Ordinary,
    Definition,
    QueryFunction,
}

impl PipelineRequest {
    pub(crate) fn of(user_context: Option<&UserContext>) -> SparkResult<Self> {
        let mut request = Self::Ordinary;
        let extensions = user_context.iter().flat_map(|user| &user.extensions);
        for extension in extensions {
            if extension.type_url.rsplit('/').next() != Some(CONTEXT_TYPE) {
                continue;
            }
            let context = PipelineAnalysisContext::decode(extension.value.as_ref())
                .map_err(|error| SparkError::invalid(format!("{CONTEXT_TYPE}: {error}")))?;
            #[expect(deprecated)]
            let flow_name = context.flow_name;
            if flow_name.is_some() {
                return Ok(Self::QueryFunction);
            }
            request = Self::Definition;
        }
        Ok(request)
    }

    /// Only `spark.sql()` may reach the server from inside a query function.
    pub(crate) fn check_execution(self, operation: &plan::OpType) -> SparkResult<()> {
        let allowed = matches!(
            operation,
            plan::OpType::Command(crate::spark::connect::Command {
                command_type: Some(command::CommandType::SqlCommand(_)),
            })
        );
        if self == Self::QueryFunction && !allowed {
            return Err(eager_work());
        }
        Ok(())
    }

    pub(crate) fn check_analysis(self) -> SparkResult<()> {
        if self == Self::QueryFunction {
            return Err(eager_work());
        }
        Ok(())
    }

    /// Reject a side-effecting statement of any pipeline definition request.
    /// Pipeline declarations have no ordinary plan, so they are parsed separately.
    pub(crate) fn parse_sql_command(self, relation: Relation) -> SparkResult<spec::Plan> {
        if self == Self::Ordinary {
            return relation.try_into();
        }
        if let Some(command) = pipeline_declaration(&relation) {
            return Err(unsupported_sql(command));
        }
        let plan: spec::Plan = relation.try_into()?;
        if let spec::Plan::Command(command) = &plan
            && !is_read_only(&command.node)
        {
            return Err(unsupported_sql(command_name(&command.node)));
        }
        Ok(plan)
    }

    /// The flow function analyzes this statement later, so nothing runs for it now.
    /// Whether a query sent through `spark.sql()` is analyzed when it is sent.
    pub(crate) fn analyzes_sql_eagerly(self) -> bool {
        matches!(self, Self::Definition)
    }

    pub(crate) fn defers_sql(self) -> bool {
        self == Self::QueryFunction
    }
}

fn pipeline_declaration(relation: &Relation) -> Option<&'static str> {
    let Some(relation::RelType::Sql(sql)) = &relation.rel_type else {
        return None;
    };
    let statement = parse_one_statement(&sql.query).ok()?;
    match from_ast_pipeline_statement(statement).ok()? {
        PipelineStatement::Dataset {
            streaming: false, ..
        } => Some("CreateMaterializedViewAsSelect"),
        PipelineStatement::Dataset {
            streaming: true, ..
        } => Some("CreateStreamingTableAsSelect"),
        _ => None,
    }
}

// The node forms of Spark's allowlist in `PipelinesHandler.blockUnsupportedSqlCommand`.
fn is_read_only(node: &spec::CommandNode) -> bool {
    matches!(
        node,
        spec::CommandNode::DescribeTable { column: None, .. }
            | spec::CommandNode::ShowCreateTable { .. }
            | spec::CommandNode::ShowTableProperties { .. }
            | spec::CommandNode::ShowTables { .. }
            | spec::CommandNode::ListDatabases { .. }
            | spec::CommandNode::ListColumns { .. }
            | spec::CommandNode::ShowFunctions { .. }
            | spec::CommandNode::ListViews { .. }
            | spec::CommandNode::ListCatalogs { .. }
    )
}

// Spark names the logical plan class. Those below are measured against Spark; any
// other command keeps Sail's own node name, which matches where the classes coincide.
fn command_name(node: &spec::CommandNode) -> String {
    let name = match node {
        spec::CommandNode::SetCurrentCatalog { .. }
        | spec::CommandNode::SetCurrentCatalogExpression { .. } => "SetCatalogCommand",
        spec::CommandNode::SetCurrentDatabase { .. } => "SetNamespaceCommand",
        spec::CommandNode::SetVariable { .. } => "SetCommand",
        spec::CommandNode::InsertInto { .. } => "InsertIntoStatement",
        spec::CommandNode::CreateDatabase { .. } => "CreateNamespace",
        spec::CommandNode::AlterTable {
            operation: spec::AlterTableOperation::RenameTable { .. },
            ..
        } => "RenameTable",
        other => {
            let debug = format!("{other:?}");
            let end = debug
                .find(|c: char| !c.is_alphanumeric())
                .unwrap_or(debug.len());
            return debug[..end].to_string();
        }
    };
    name.to_string()
}

fn eager_work() -> SparkError {
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION".into(),
        sql_state: "0A000".into(),
        message: "Operations that trigger DataFrame analysis or execution are not allowed in pipeline query functions. Move code outside of the pipeline query function.".into(),
        parameters: HashMap::new(),
    })
    .into()
}

fn unsupported_sql(command: impl Into<String>) -> SparkError {
    let command = command.into();
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND".into(),
        sql_state: "0A000".into(),
        message: format!(
            "'{command}' is not supported in spark.sql(\"...\") API in Spark Declarative Pipeline."
        ),
        parameters: HashMap::from([("command".into(), command)]),
    })
    .into()
}

#[cfg(test)]
#[path = "query_function_guard_tests.rs"]
mod tests;
