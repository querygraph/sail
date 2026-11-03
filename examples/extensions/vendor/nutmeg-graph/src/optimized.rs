//! Nutmeg-local experimental kernels, distinct from the Grust references.
//!
//! Data buffers are admitted before allocation; exported buffers retain their
//! own reservation. Per-read worker pools obey the admitted execution's width;
//! floating reductions use fixed input partitions, independent of that width.
use super::*;
use grust_procedures::{
    Argument, Correlation, Determinism, GraphRequirement, Invocation, OptionField, ProcedureCursor,
    ProcedureMode, ProcedureProvider, Streaming,
};

mod pagerank;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_wcc_fused;
mod wcc;

pub(super) const NAMES: [&str; 3] = ["pagerankDelta", "wccRandomized", "wccRandomizedFused"];

fn field(name: &str, kind: ValueType) -> grust_procedures::Field {
    grust_procedures::Field {
        name: name.into(),
        value_type: kind,
        nullable: false,
    }
}

fn option(name: &str, kind: ValueType, default: Value) -> OptionField {
    let mut field = field(name, kind);
    field.nullable = matches!(default, Value::Null);
    OptionField {
        field,
        default: Some(default),
    }
}

struct CatalogOnly;
impl ProcedureProvider for CatalogOnly {
    fn open<'a>(
        &'a self,
        _: ValidatedArguments,
        _: Invocation<'a>,
    ) -> grust_procedures::Result<Box<dyn ProcedureCursor + 'a>> {
        Err(ProcedureError::Unsupported(
            "Nutmeg kernels require its projection runner".into(),
        ))
    }
}

pub(super) fn register(builder: &mut RegistryBuilder) -> grust_procedures::Result<()> {
    for name in NAMES {
        let mut options = vec![
            option(
                "orientation",
                ValueType::String,
                Value::String("outgoing".into()),
            ),
            option("nodeLabels", ValueType::Strings, Value::Null),
            option("relationshipTypes", ValueType::Strings, Value::Null),
            option("weightProperty", ValueType::String, Value::Null),
            option("defaultWeight", ValueType::Number, Value::Null),
            option("maxIterations", ValueType::Integer, Value::Int(1000)),
        ];
        let mut outputs = vec![field("nodeId", ValueType::String)];
        if name == "pagerankDelta" {
            options.extend([
                option("damping", ValueType::Number, Value::Float(0.85)),
                option("tolerance", ValueType::Number, Value::Float(1e-8)),
                option("precision", ValueType::String, Value::String("f64".into())),
            ]);
            outputs.extend([
                field("score", ValueType::Number),
                field("iterations", ValueType::Integer),
                field("converged", ValueType::Boolean),
                field("residual", ValueType::Number),
            ]);
        } else {
            options.push(option("seed", ValueType::Integer, Value::Int(42)));
            outputs.push(field("componentId", ValueType::String));
        }
        builder.register(
            ProcedureDefinition {
                name: format!("{PREFIX}{name}"),
                aliases: vec![],
                version: 1,
                provider: "nutmeg.experimental".into(),
                arguments: vec![Argument {
                    field: field("config", ValueType::Map),
                    default: Some(Value::Json(serde_json::json!({}))),
                }],
                options_argument: Some(0),
                options,
                outputs,
                mode: ProcedureMode::Read,
                determinism: Determinism::Deterministic,
                correlation: Correlation::Independent,
                graph: GraphRequirement::LocalSnapshot,
                streaming: Streaming::Blocking,
            },
            Arc::new(CatalogOnly),
        )?;
    }
    Ok(())
}

pub(super) fn schema(algorithm: &str) -> Option<SchemaRef> {
    let mut fields = vec![Field::new("nodeId", DataType::Utf8, false)];
    match algorithm {
        "pagerankDelta" => fields.extend([
            Field::new("score", DataType::Float64, false),
            Field::new("iterations", DataType::Int64, false),
            Field::new("converged", DataType::Boolean, false),
            Field::new("residual", DataType::Float64, false),
        ]),
        "wccRandomized" | "wccRandomizedFused" => {
            fields.push(Field::new("componentId", DataType::Utf8, false))
        }
        _ => return None,
    }
    Some(Arc::new(Schema::new(fields)))
}

fn integer(args: &ValidatedArguments, key: &str) -> Result<i64> {
    match args.options().get(key) {
        Some(Value::Int(value)) => Ok(*value),
        _ => plan_err!("nutmeg: {key} must be an integer"),
    }
}

fn number(args: &ValidatedArguments, key: &str) -> Result<f64> {
    let value = match args.options().get(key) {
        Some(Value::Float(value)) => *value,
        Some(Value::Int(value)) => *value as f64,
        _ => return plan_err!("nutmeg: {key} must be a number"),
    };
    if !value.is_finite() {
        return plan_err!("nutmeg: {key} must be finite");
    }
    Ok(value)
}

/// JSON can carry exact u64 seeds; the procedure value domain uses i64.
/// Convert only integer JSON numbers, before typed validation, never via f64.
pub(super) fn normalize_seed(
    algorithm: &str,
    options: &mut serde_json::Map<String, serde_json::Value>,
) {
    if (algorithm.eq_ignore_ascii_case("wccRandomized")
        || algorithm.eq_ignore_ascii_case("wccRandomizedFused"))
        && let Some(seed) = options.get("seed").and_then(serde_json::Value::as_u64)
        && seed > i64::MAX as u64
    {
        options.insert("seed".into(), serde_json::json!(seed as i64));
    }
}

pub(super) fn check_options(algorithm: &str, args: &ValidatedArguments) -> Result<()> {
    let Some(&algorithm) = NAMES
        .iter()
        .find(|name| name.eq_ignore_ascii_case(algorithm))
    else {
        return Ok(());
    };
    let iterations = integer(args, "maxIterations")?;
    if !(1..=10000).contains(&iterations) {
        return plan_err!("nutmeg: optimized maxIterations must be in 1..=10000");
    }
    if !matches!(args.options().get("orientation"), Some(Value::String(s)) if s == "outgoing")
        || !matches!(args.options().get("weightProperty"), Some(Value::Null))
        || !matches!(args.options().get("defaultWeight"), Some(Value::Null))
    {
        return plan_err!("nutmeg: optimized kernels require an unweighted outgoing projection");
    }
    if algorithm == "pagerankDelta" {
        if !(0.0..1.0).contains(&number(args, "damping")?) || number(args, "tolerance")? <= 0.0 {
            return plan_err!("nutmeg: require 0 <= damping < 1 and positive tolerance");
        }
        if !matches!(args.options().get("precision"), Some(Value::String(s)) if s == "f64") {
            return plan_err!("nutmeg: pagerankDelta supports precision=f64 only");
        }
    }
    Ok(())
}

fn allocated<T: Clone>(length: usize, value: T) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values.try_reserve_exact(length).map_err(err)?;
    values.resize(length, value);
    Ok(values)
}

fn admitted(
    context: &ExecutionContext,
    nodes: usize,
    edges: usize,
    node_width: usize,
    edge_width: usize,
) -> Result<MemoryReservation> {
    let bytes = nodes
        .saturating_mul(node_width)
        .saturating_add(edges.saturating_mul(edge_width))
        .saturating_add(4096);
    context.reserve(bytes).map_err(err)
}

const BLOCK: usize = 4096;

fn pool(context: &ExecutionContext, nodes: usize) -> Result<(Option<rayon::ThreadPool>, usize)> {
    let threads = context.concurrency().min(nodes.div_ceil(BLOCK).max(1));
    if threads == 1 {
        return Ok((None, 1));
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("nutmeg-optimized-{i}"))
        .build()
        .map_err(err)?;
    Ok((Some(pool), threads))
}

pub(super) fn run(
    algorithm: &str,
    graph: &GraphProjection,
    args: &ValidatedArguments,
    query: &Query,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    check_options(algorithm, args)?;
    if graph.is_weighted() || graph.orientation() != grust_algorithms::Orientation::Outgoing {
        return exec_err!("nutmeg: optimized kernels require unweighted outgoing edges");
    }
    match algorithm {
        "pagerankDelta" => pagerank::run(graph, args, query, emit),
        "wccRandomized" | "wccRandomizedFused" => wcc::run(algorithm, graph, args, query, emit),
        _ => internal_err!("unknown optimized kernel"),
    }
}

fn save_diagnostics(query: &Query, value: serde_json::Value) -> Result<()> {
    *query.diagnostics.lock().map_err(|_| poisoned())? = value;
    Ok(())
}

/// Reservation accompanies Arrow buffers across both the bounded channel and FFI.
fn emit_result(
    graph: &GraphProjection,
    algorithm: &str,
    scores: Option<(&[f64], usize, bool, f64)>,
    components: Option<&[i64]>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    let context = graph.execution();
    let n = graph.node_count();
    let batch_rows = context.limits().batch_rows;
    let schema = schema(algorithm).ok_or_else(|| err("missing optimized schema"))?;
    for start in (0..n.max(1)).step_by(batch_rows) {
        context.checkpoint().map_err(err)?;
        let end = n.min(start.saturating_add(batch_rows));
        let ids = &graph.node_ids()[start..end];
        let text_bytes = ids.iter().map(|id| id.as_str().len()).sum::<usize>();
        let bound = text_bytes
            .saturating_add(ids.len().saturating_mul(128))
            .saturating_mul(4)
            .saturating_add(4096);
        let reservation = Arc::new(context.reserve(bound).map_err(err)?);
        let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from_iter_values(
            ids.iter().map(|id| id.as_str()),
        ))];
        if let Some((scores, iterations, converged, residual)) = scores {
            columns.extend([
                Arc::new(Float64Array::from(scores[start..end].to_vec())) as ArrayRef,
                Arc::new(Int64Array::from(vec![iterations as i64; ids.len()])),
                Arc::new(BooleanArray::from(vec![converged; ids.len()])),
                Arc::new(Float64Array::from(vec![residual; ids.len()])),
            ]);
        } else if let Some(components) = components {
            columns.push(Arc::new(StringArray::from_iter_values(
                components[start..end].iter().map(i64::to_string),
            )));
        }
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        if !emit(graph_tables::retain_owner(batch, reservation)?)? {
            return Ok(false);
        }
    }
    Ok(true)
}
