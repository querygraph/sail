//! Spark 4.2.1 Auto CDC flow arguments and their definition-time validation.
//!
//! This ports the argument contract of `autocdc/ChangeArgs.scala`, the
//! `PipelinesHandler.buildAutoCdcFlow` checks, the schema-level validators of
//! `graph/Flow.scala` (`AutoCdcMergeFlow`) and the Auto CDC rules of
//! `graph/GraphValidations.scala`. Nothing here executes: the auxiliary state
//! table, reconciliation and MERGE effects are later units, and a run with a
//! streaming-table output is still rejected before any write in
//! `execution::execute`. Dry runs analyze the flow as a streaming read of its
//! source, like Spark's `AutoCdcFlow` function.
//!
//! Definition-time checks live here; the resolution-time validators that
//! need the source field names are in [`schema`], and the Spark error
//! conditions in [`errors`].

mod attribute;
pub(super) mod auxiliary;
mod dedup;
mod errors;
mod lookup;
mod microbatch;
mod prepared;
mod schema;
#[cfg(test)]
mod tests;

use sail_common::spec;
use sail_sql_analyzer::expression::from_ast_object_name;
use sail_sql_analyzer::parser::parse_object_name;

use super::graph::{Flow, GraphDefinition, Output, quoted};
use crate::error::{SparkError, SparkResult};
use crate::spark::connect::expression::ExprType;
use crate::spark::connect::pipeline_command::define_flow::AutoCdcFlowDetails;
use crate::spark::connect::{self as sc, OutputType};

/// The SCD strategy of a flow (`ScdType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScdType {
    Type1,
    /// The proto has no SCD2 value; Spark rejects the type at planning.
    #[cfg_attr(not(test), expect(dead_code, reason = "no proto value reaches it"))]
    Type2,
}

/// A single unqualified column identifier (`UnqualifiedColumnName`).
/// Backticks are consumed: `` `a.b` `` is stored as `a.b`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct UnqualifiedColumnName {
    name: String,
}

impl UnqualifiedColumnName {
    pub fn from_parts(parts: Vec<String>) -> SparkResult<Self> {
        match <[String; 1]>::try_from(parts) {
            Ok([name]) => Ok(Self { name }),
            Err(parts) => Err(errors::multipart_column_identifier(
                &parts.join("."),
                &parts,
            )),
        }
    }
}

/// Which source columns reach the target (`ColumnSelection`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ColumnSelection {
    Include(Vec<UnqualifiedColumnName>),
    Exclude(Vec<UnqualifiedColumnName>),
}

/// Configuration of an Auto CDC flow (`ChangeArgs`).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ChangeArgs {
    pub keys: Vec<UnqualifiedColumnName>,
    pub sequencing: spec::Expr,
    pub scd_type: ScdType,
    pub delete_condition: Option<spec::Expr>,
    pub column_selection: Option<ColumnSelection>,
}

impl ChangeArgs {
    /// Construction validates `keys` is non-empty, as the Scala case class does.
    pub fn new(
        keys: Vec<UnqualifiedColumnName>,
        sequencing: spec::Expr,
        scd_type: ScdType,
        delete_condition: Option<spec::Expr>,
        column_selection: Option<ColumnSelection>,
    ) -> SparkResult<Self> {
        if keys.is_empty() {
            return Err(errors::empty_keys());
        }
        Ok(Self {
            keys,
            sequencing,
            scd_type,
            delete_condition,
            column_selection,
        })
    }
}

/// A validated Auto CDC flow definition: the parsed source name and the
/// change arguments.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Definition {
    pub source: Vec<String>,
    pub change_args: ChangeArgs,
}

impl Definition {
    /// The flow function Spark registers: a streaming read of the source,
    /// resolved against the graph at analysis time.
    pub fn query(&self) -> spec::QueryPlan {
        spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                name: spec::ObjectName::from(self.source.clone()),
                temporal: None,
                sample: None,
                options: vec![],
            })),
            is_streaming: true,
        })
    }
}

/// `PipelinesHandler.buildAutoCdcFlow`: validates the proto details in
/// Spark's order and returns the definition.
pub(super) fn definition(details: AutoCdcFlowDetails) -> SparkResult<Definition> {
    let AutoCdcFlowDetails {
        source,
        keys,
        sequence_by,
        apply_as_deletes,
        column_list,
        except_column_list,
        stored_as_scd_type,
        ..
    } = details;
    let Some(source) = source else {
        return Err(errors::missing_source());
    };
    let Some(sequence_by) = sequence_by else {
        return Err(errors::missing_sequence_by());
    };
    if source.trim().is_empty() {
        return Err(errors::parse_empty_statement());
    }
    let source = parse_source(&source)?;
    let keys = keys
        .into_iter()
        .map(unqualified_column_name)
        .collect::<SparkResult<Vec<_>>>()?;
    let column_selection = match (column_list.is_empty(), except_column_list.is_empty()) {
        (false, false) => return Err(errors::both_column_list_and_except_column_list()),
        (false, true) => Some(ColumnSelection::Include(
            column_list
                .into_iter()
                .map(unqualified_column_name)
                .collect::<SparkResult<Vec<_>>>()?,
        )),
        (true, false) => Some(ColumnSelection::Exclude(
            except_column_list
                .into_iter()
                .map(unqualified_column_name)
                .collect::<SparkResult<Vec<_>>>()?,
        )),
        (true, true) => None,
    };
    // An unspecified type defaults to SCD1. The proto has no SCD2 value; an
    // unknown number is what Java reports as `UNRECOGNIZED`.
    let scd_type = match stored_as_scd_type {
        0 | 1 => ScdType::Type1,
        _ => {
            return Err(SparkError::unsupported(
                "Unsupported AutoCDC SCD type: UNRECOGNIZED",
            ));
        }
    };
    let sequencing = spec::Expr::try_from(sequence_by)?;
    let delete_condition = apply_as_deletes.map(spec::Expr::try_from).transpose()?;
    let change_args = ChangeArgs::new(
        keys,
        sequencing,
        scd_type,
        delete_condition,
        column_selection,
    )?;
    Ok(Definition {
        source,
        change_args,
    })
}

/// `GraphIdentifierManager.parseTableIdentifier`: the SQL multipart parser,
/// then `toTableIdentifier`, which accepts one to three parts and reports the
/// parts as parsed. Accepted names are normalized like `graph::parse_name`.
fn parse_source(source: &str) -> SparkResult<Vec<String>> {
    let parts: Vec<String> = from_ast_object_name(parse_object_name(source)?)?.into();
    if parts.len() > 3 {
        return Err(errors::four_part_table_identifier(&parts));
    }
    Ok(parts
        .into_iter()
        .map(|part| part.to_ascii_lowercase())
        .collect())
}

/// `PipelinesHandler.asUnqualifiedColumnName` after the planner's
/// `transformExpression`: a proto attribute goes through Spark's attribute
/// name parser (`UnresolvedAttribute.quotedString`), any other expression
/// through Sail's converter, and only an unresolved attribute is accepted.
fn unqualified_column_name(expression: sc::Expression) -> SparkResult<UnqualifiedColumnName> {
    if let Some(ExprType::UnresolvedAttribute(attribute)) = &expression.expr_type {
        let parts = attribute::parse_attribute_name(&attribute.unparsed_identifier)?;
        return UnqualifiedColumnName::from_parts(parts);
    }
    match spec::Expr::try_from(expression)? {
        spec::Expr::UnresolvedAttribute { name, .. } => {
            UnqualifiedColumnName::from_parts(name.into())
        }
        other => Err(errors::non_column_identifier(&expression_sql(&other))),
    }
}

/// A best-effort rendering of Catalyst `Expression.sql` for error messages.
fn expression_sql(expression: &spec::Expr) -> String {
    match expression {
        spec::Expr::UnresolvedAttribute { name, .. } => Vec::<String>::from(name.clone()).join("."),
        spec::Expr::Literal(literal) => literal_sql(literal),
        spec::Expr::UnresolvedFunction(function) => {
            let name = Vec::<String>::from(function.function_name.clone()).join(".");
            let arguments: Vec<String> = function.arguments.iter().map(expression_sql).collect();
            match (name.as_str(), arguments.as_slice()) {
                ("not", [argument]) => format!("(NOT {argument})"),
                ("and" | "or", [left, right]) => {
                    format!("({left} {} {right})", name.to_ascii_uppercase())
                }
                (
                    "+" | "-" | "*" | "/" | "%" | ">" | "<" | ">=" | "<=" | "=" | "==" | "!="
                    | "<>",
                    [left, right],
                ) => format!("({left} {name} {right})"),
                _ => format!("{name}({})", arguments.join(", ")),
            }
        }
        spec::Expr::Alias { expr, .. } => expression_sql(expr),
        other => format!("{other:?}"),
    }
}

fn literal_sql(literal: &spec::Literal) -> String {
    use spec::Literal;
    match literal {
        Literal::Null => "NULL".into(),
        Literal::Boolean { value: Some(value) } => value.to_string().to_ascii_uppercase(),
        Literal::Int8 { value: Some(value) } => value.to_string(),
        Literal::Int16 { value: Some(value) } => value.to_string(),
        Literal::Int32 { value: Some(value) } => value.to_string(),
        Literal::Int64 { value: Some(value) } => value.to_string(),
        Literal::Float64 { value: Some(value) } => value.to_string(),
        Literal::Utf8 { value: Some(value) } => format!("'{}'", value.replace('\'', "\\'")),
        other => format!("{other:?}"),
    }
}

/// `GraphValidations.validateMultiQueryTables`, first clause, over the whole
/// graph: every Auto CDC multiflow destination is reported before any
/// materialized-view multiflow one. Called once every flow has resolved.
pub(super) fn validate_multiple_flows_globally(graph: &GraphDefinition) -> SparkResult<()> {
    for output in &graph.outputs {
        let flows: Vec<&Flow> = graph
            .flows
            .iter()
            .filter(|flow| flow.target == output.name)
            .collect();
        validate_multiple_flows(output, &flows)?;
    }
    Ok(())
}

/// One destination's Auto CDC multiflow clause.
pub(super) fn validate_multiple_flows(output: &Output, flows: &[&Flow]) -> SparkResult<()> {
    if flows.len() > 1 && flows.iter().any(|flow| flow.auto_cdc.is_some()) {
        let mut names: Vec<String> = flows.iter().map(|flow| flow.name.join(".")).collect();
        names.sort();
        return Err(errors::multiple_flows_to_target(
            &output.name.join("."),
            &names.join(", "),
        ));
    }
    Ok(())
}

/// `GraphValidations.validateFlowStreamingness`, temporary-view clause: an
/// Auto CDC flow may not target a temporary view. Spark checks resolved
/// flows only, after the multiquery and resettability phases; the analysis
/// loop calls this once the graph has resolved. A sink destination is
/// rejected by Spark's `FlowPlanner` only when a flow is planned for
/// execution, which dry runs never reach, so no sink check exists here.
pub(super) fn validate_temporary_views(graph: &GraphDefinition) -> SparkResult<()> {
    for output in graph
        .outputs
        .iter()
        .filter(|output| output.kind == OutputType::TemporaryView)
    {
        if let Some(flow) = graph
            .flows
            .iter()
            .find(|flow| flow.target == output.name && flow.auto_cdc.is_some())
        {
            return Err(errors::relation_for_temporary_view(
                &quoted(&flow.name),
                &quoted(&output.name),
            ));
        }
    }
    Ok(())
}
