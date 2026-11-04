use datafusion::common::{Result, plan_datafusion_err};
use sail_orc::options::r#gen::OrcReadOptions;

use crate::plan::r#gen;

pub(super) fn encode_read_options(options: &OrcReadOptions) -> Result<r#gen::OrcReadOptions> {
    let encode = |value, name| {
        u64::try_from(value)
            .map(Some)
            .map_err(|_| plan_datafusion_err!("ORC read option {name} exceeds uint64"))
    };
    Ok(r#gen::OrcReadOptions {
        max_encoded_metadata_bytes: encode(
            options.max_encoded_metadata_bytes,
            "max_encoded_metadata_bytes",
        )?,
        max_decoded_metadata_bytes: encode(
            options.max_decoded_metadata_bytes,
            "max_decoded_metadata_bytes",
        )?,
        max_decoded_stripe_footer_bytes: encode(
            options.max_decoded_stripe_footer_bytes,
            "max_decoded_stripe_footer_bytes",
        )?,
        max_schema_depth: encode(options.max_schema_depth, "max_schema_depth")?,
    })
}

pub(super) fn decode_read_options(options: &r#gen::OrcReadOptions) -> Result<OrcReadOptions> {
    let decode = |value: Option<u64>, name| {
        let value = value.ok_or_else(|| plan_datafusion_err!("Missing ORC read option {name}"))?;
        usize::try_from(value)
            .map_err(|_| plan_datafusion_err!("ORC read option {name} exceeds usize"))
    };
    Ok(OrcReadOptions {
        max_encoded_metadata_bytes: decode(
            options.max_encoded_metadata_bytes,
            "max_encoded_metadata_bytes",
        )?,
        max_decoded_metadata_bytes: decode(
            options.max_decoded_metadata_bytes,
            "max_decoded_metadata_bytes",
        )?,
        max_decoded_stripe_footer_bytes: decode(
            options.max_decoded_stripe_footer_bytes,
            "max_decoded_stripe_footer_bytes",
        )?,
        max_schema_depth: decode(options.max_schema_depth, "max_schema_depth")?,
    })
}
