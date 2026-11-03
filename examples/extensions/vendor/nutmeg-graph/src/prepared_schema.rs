//! Eager, bounded schema setup for native extension session binding.
use super::*;

/// Prepare the finite output-schema catalog before accepting user plans.
///
/// Reference result types come from Grust and are observed on a fixed three-node
/// probe graph under a separate 16 MiB budget. The two local optimized kernels
/// supply static schemas. Setup runs once per library instance; defaults and
/// supported rank precisions define a finite cache. Session reads use only it.
pub fn prepare_output_schemas() -> Result<()> {
    static PREPARED: OnceCell<()> = OnceCell::new();
    PREPARED
        .get_or_try_init(|| {
            for definition in definitions() {
                let algorithm = short(definition);
                output_schema_at(algorithm, None)?;
                if definition
                    .options
                    .iter()
                    .any(|o| o.field.name == PRECISION_OPTION)
                {
                    for precision in ["f32", "f64"] {
                        if algorithm == "pagerankDelta" && precision != "f64" {
                            continue;
                        }
                        output_schema_at(algorithm, Some(precision))?;
                    }
                }
            }
            Ok(())
        })
        .map(|_| ())
}

pub(super) fn cached_output_schema(
    algorithm: &str,
    args: &ValidatedArguments,
    names: ColumnNames,
) -> Result<SchemaRef> {
    let precision = precision_of(algorithm, args)?;
    let key = match precision.as_deref() {
        None => algorithm.to_owned(),
        Some(value @ ("f32" | "f64")) => format!("{algorithm}#{PRECISION_OPTION}={value}"),
        Some(other) => {
            return plan_err!(
                "nutmeg: unsupported output precision `{other}`; expected f32 or f64"
            );
        }
    };
    let schemas = SCHEMAS.read().map_err(|_| poisoned())?;
    let Some(schema) = schemas.get(&key) else {
        return plan_err!("nutmeg: schema `{key}` was not prepared during session binding");
    };
    names.rename_schema(schema)
}
