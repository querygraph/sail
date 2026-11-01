//! The archival envelope, ported from the sail-iggy reader
//! (`formats/iggy/envelope.rs` at `5816b9b`).

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, Int64Array, LargeBinaryBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use iggy::prelude::{HeaderKey, HeaderValue, IggyMessage};

pub const SCHEMA_REGISTRY_HEADER: &str = "sail-schema-registry";
pub const SCHEMA_MODULE_HEADER: &str = "sail-schema-module";
pub const SCHEMA_COMMIT_HEADER: &str = "sail-schema-commit";
pub const SCHEMA_MESSAGE_HEADER: &str = "sail-schema-message";
pub const SCHEMA_DIGEST_HEADER: &str = "sail-schema-digest";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct IggySchemaReference {
    pub registry: String,
    pub module: String,
    pub commit: String,
    pub message: String,
    pub digest: String,
}

impl IggySchemaReference {
    pub fn try_new(
        registry: String,
        module: String,
        commit: String,
        message: String,
        digest: String,
    ) -> Result<Self> {
        let reference = Self {
            registry,
            module,
            commit,
            message,
            digest,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub fn from_optional_parts(
        registry: Option<String>,
        module: Option<String>,
        commit: Option<String>,
        message: Option<String>,
        digest: Option<String>,
    ) -> Result<Option<Self>> {
        match (registry, module, commit, message, digest) {
            (None, None, None, None, None) => Ok(None),
            (Some(registry), Some(module), Some(commit), Some(message), Some(digest)) => {
                Self::try_new(registry, module, commit, message, digest).map(Some)
            }
            _ => Err(DataFusionError::Plan(
                "Iggy schema reference options must be specified together: schema_registry, schema_module, schema_commit, schema_message, and schema_digest"
                    .to_string(),
            )),
        }
    }

    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("schema_registry", self.registry.as_str()),
            ("schema_module", self.module.as_str()),
            ("schema_commit", self.commit.as_str()),
            ("schema_message", self.message.as_str()),
            ("schema_digest", self.digest.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "Iggy {name} must not be empty"
                )));
            }
        }
        let digest = self.digest.strip_prefix("sha256:").ok_or_else(|| {
            DataFusionError::Plan(
                "Iggy schema_digest must use sha256:<64 hexadecimal characters>".to_string(),
            )
        })?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value))
        {
            return Err(DataFusionError::Plan(
                "Iggy schema_digest must use canonical lowercase sha256:<64 hexadecimal characters>"
                    .to_string(),
            ));
        }
        if self.registry.eq_ignore_ascii_case("buf.build")
            && (self.commit.len() != 32
                || !self
                    .commit
                    .bytes()
                    .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)))
        {
            return Err(DataFusionError::Plan(
                "Iggy schema_commit for buf.build must be a 32-character lowercase immutable BSR commit ID, not a label"
                    .to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn to_user_headers(&self) -> Result<BTreeMap<HeaderKey, HeaderValue>> {
        self.validate()?;
        [
            (SCHEMA_REGISTRY_HEADER, self.registry.as_str()),
            (SCHEMA_MODULE_HEADER, self.module.as_str()),
            (SCHEMA_COMMIT_HEADER, self.commit.as_str()),
            (SCHEMA_MESSAGE_HEADER, self.message.as_str()),
            (SCHEMA_DIGEST_HEADER, self.digest.as_str()),
        ]
        .into_iter()
        .map(|(key, value)| {
            let key = HeaderKey::from_str(key).map_err(|error| {
                DataFusionError::Execution(format!("invalid Iggy schema header key: {error}"))
            })?;
            let value = HeaderValue::from_str(value).map_err(|error| {
                DataFusionError::Execution(format!("invalid Iggy schema header value: {error}"))
            })?;
            Ok((key, value))
        })
        .collect()
    }
}

pub fn parse_schema_reference(
    headers: &BTreeMap<HeaderKey, HeaderValue>,
) -> Result<Option<IggySchemaReference>> {
    let registry = header_string(headers, SCHEMA_REGISTRY_HEADER)?;
    let module = header_string(headers, SCHEMA_MODULE_HEADER)?;
    let commit = header_string(headers, SCHEMA_COMMIT_HEADER)?;
    let message = header_string(headers, SCHEMA_MESSAGE_HEADER)?;
    let digest = header_string(headers, SCHEMA_DIGEST_HEADER)?;
    IggySchemaReference::from_optional_parts(registry, module, commit, message, digest)
}

pub fn schema_reference_from_message(message: &IggyMessage) -> Result<Option<IggySchemaReference>> {
    let headers = message.user_headers_map().map_err(|error| {
        DataFusionError::Execution(format!("invalid Iggy user headers: {error}"))
    })?;
    headers
        .as_ref()
        .map(parse_schema_reference)
        .transpose()
        .map(Option::flatten)
}

fn header_string(headers: &BTreeMap<HeaderKey, HeaderValue>, name: &str) -> Result<Option<String>> {
    let key = HeaderKey::from_str(name).map_err(|error| {
        DataFusionError::Execution(format!("invalid Iggy schema header key: {error}"))
    })?;
    headers
        .get(&key)
        .map(|value| {
            value.as_str().map(str::to_owned).map_err(|error| {
                DataFusionError::Execution(format!(
                    "Iggy schema header {name} must be a string: {error}"
                ))
            })
        })
        .transpose()
}

pub fn envelope_schema() -> Schema {
    Schema::new(vec![
        Arc::new(Field::new("source_id", DataType::Utf8, false)),
        Arc::new(Field::new("stream", DataType::Utf8, false)),
        Arc::new(Field::new("topic", DataType::Utf8, false)),
        Arc::new(Field::new("partition", DataType::Int64, false)),
        Arc::new(Field::new("offset", DataType::Int64, false)),
        Arc::new(Field::new(
            "broker_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )),
        Arc::new(Field::new(
            "origin_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )),
        Arc::new(Field::new("message_id", DataType::Binary, false)),
        Arc::new(Field::new("checksum", DataType::Binary, false)),
        Arc::new(Field::new("payload", DataType::LargeBinary, false)),
        Arc::new(Field::new("user_headers", DataType::LargeBinary, true)),
        Arc::new(Field::new("schema_registry", DataType::Utf8, true)),
        Arc::new(Field::new("schema_module", DataType::Utf8, true)),
        Arc::new(Field::new("schema_commit", DataType::Utf8, true)),
        Arc::new(Field::new("schema_message", DataType::Utf8, true)),
        Arc::new(Field::new("schema_digest", DataType::Utf8, true)),
    ])
}

pub fn validate_envelope_schema(schema: &Schema) -> Result<()> {
    let expected = envelope_schema();
    if schema.fields() != expected.fields() {
        return Err(DataFusionError::Plan(
            "invalid envelope Iggy schema".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct IggyEnvelopeContext<'a> {
    pub source_id: &'a str,
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition: u32,
}

pub fn envelope_batch(
    schema: SchemaRef,
    context: IggyEnvelopeContext<'_>,
    messages: &[IggyMessage],
) -> Result<RecordBatch> {
    validate_envelope_schema(&schema)?;
    let len = messages.len();
    let source_ids = StringArray::from_iter_values(std::iter::repeat_n(context.source_id, len));
    let streams = StringArray::from_iter_values(std::iter::repeat_n(context.stream, len));
    let topics = StringArray::from_iter_values(std::iter::repeat_n(context.topic, len));
    let partitions = Int64Array::from_value(i64::from(context.partition), len);
    let offsets = Int64Array::from(
        messages
            .iter()
            .map(|message| i64::try_from(message.header.offset))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| DataFusionError::Execution("Iggy offset exceeds Int64".to_string()))?,
    );
    let broker_timestamps =
        timestamp_array(messages.iter().map(|message| message.header.timestamp))?;
    let origin_timestamps = timestamp_array(
        messages
            .iter()
            .map(|message| message.header.origin_timestamp),
    )?;

    let mut message_ids = BinaryBuilder::new();
    let mut checksums = BinaryBuilder::new();
    let mut payloads = LargeBinaryBuilder::new();
    let mut user_headers = LargeBinaryBuilder::new();
    let mut schema_registries = StringBuilder::new();
    let mut schema_modules = StringBuilder::new();
    let mut schema_commits = StringBuilder::new();
    let mut schema_messages = StringBuilder::new();
    let mut schema_digests = StringBuilder::new();

    for message in messages {
        message_ids.append_value(message.header.id.to_le_bytes());
        checksums.append_value(message.header.checksum.to_le_bytes());
        payloads.append_value(message.payload.as_ref());
        user_headers.append_option(message.user_headers.as_deref());

        // The raw headers remain authoritative. A malformed or partial schema
        // reference must not make an otherwise valid archive message unreadable.
        let reference = schema_reference_from_message(message).ok().flatten();
        schema_registries.append_option(reference.as_ref().map(|value| value.registry.as_str()));
        schema_modules.append_option(reference.as_ref().map(|value| value.module.as_str()));
        schema_commits.append_option(reference.as_ref().map(|value| value.commit.as_str()));
        schema_messages.append_option(reference.as_ref().map(|value| value.message.as_str()));
        schema_digests.append_option(reference.as_ref().map(|value| value.digest.as_str()));
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(source_ids),
        Arc::new(streams),
        Arc::new(topics),
        Arc::new(partitions),
        Arc::new(offsets),
        Arc::new(broker_timestamps),
        Arc::new(origin_timestamps),
        Arc::new(message_ids.finish()),
        Arc::new(checksums.finish()),
        Arc::new(payloads.finish()),
        Arc::new(user_headers.finish()),
        Arc::new(schema_registries.finish()),
        Arc::new(schema_modules.finish()),
        Arc::new(schema_commits.finish()),
        Arc::new(schema_messages.finish()),
        Arc::new(schema_digests.finish()),
    ];
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

fn timestamp_array(values: impl Iterator<Item = u64>) -> Result<TimestampNanosecondArray> {
    let values = values
        .map(|value| {
            i64::try_from(value)
                .ok()
                .and_then(|value| value.checked_mul(1_000))
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Iggy timestamp exceeds nanosecond range".to_string(),
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(TimestampNanosecondArray::from(values).with_timezone("UTC"))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use bytes::Bytes;
    use datafusion::arrow::array::{Array, BinaryArray, LargeBinaryArray};

    use super::*;

    fn reference() -> IggySchemaReference {
        IggySchemaReference::try_new(
            "buf.build".to_string(),
            "buf.build/querygraph/orders".to_string(),
            "d39267d9df8f4053bbac6b956a23169f".to_string(),
            "querygraph.orders.v1.Order".to_string(),
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        )
        .unwrap()
    }

    #[test]
    fn schema_reference_headers_round_trip() {
        let expected = reference();
        let headers = expected.to_user_headers().unwrap();
        assert_eq!(parse_schema_reference(&headers).unwrap(), Some(expected));
    }

    #[test]
    fn schema_reference_requires_all_parts() {
        let error = IggySchemaReference::from_optional_parts(
            Some("buf.build".to_string()),
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be specified together"));
    }

    #[test]
    fn schema_reference_requires_a_full_sha256_digest() {
        let error = IggySchemaReference::try_new(
            "buf.build".to_string(),
            "buf.build/querygraph/orders".to_string(),
            "d39267d9df8f4053bbac6b956a23169f".to_string(),
            "querygraph.orders.v1.Order".to_string(),
            "sha256:0123456789abcdef".to_string(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("64 hexadecimal characters"));
    }

    #[test]
    fn buf_schema_reference_rejects_mutable_label() {
        let error = IggySchemaReference::try_new(
            "buf.build".to_string(),
            "buf.build/querygraph/orders".to_string(),
            "main".to_string(),
            "querygraph.orders.v1.Order".to_string(),
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("immutable BSR commit ID"));
    }

    #[test]
    fn envelope_preserves_message_and_schema_metadata() {
        let mut message = IggyMessage::builder()
            .id(42)
            .payload(Bytes::from_static(b"payload"))
            .user_headers(reference().to_user_headers().unwrap())
            .build()
            .unwrap();
        message.header.offset = 17;
        message.header.timestamp = 1_000;
        message.header.origin_timestamp = 900;
        message.header.checksum = 0x0102_0304_0506_0708;

        let batch = envelope_batch(
            Arc::new(envelope_schema()),
            IggyEnvelopeContext {
                source_id: "iggy-production",
                stream: "events",
                topic: "orders",
                partition: 3,
            },
            &[message],
        )
        .unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("source_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "iggy-production"
        );
        assert_eq!(
            batch
                .column_by_name("payload")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            b"payload"
        );
        assert_eq!(
            batch
                .column_by_name("message_id")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            42_u128.to_le_bytes()
        );
        let modules = batch
            .column_by_name("schema_module")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(modules.value(0), "buf.build/querygraph/orders");
    }

    #[test]
    fn envelope_keeps_raw_headers_when_schema_reference_is_partial() {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_str(SCHEMA_REGISTRY_HEADER).unwrap(),
            HeaderValue::from_str("buf.build").unwrap(),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"payload"))
            .user_headers(headers)
            .build()
            .unwrap();
        let batch = envelope_batch(
            Arc::new(envelope_schema()),
            IggyEnvelopeContext {
                source_id: "iggy-production",
                stream: "events",
                topic: "orders",
                partition: 0,
            },
            &[message],
        )
        .unwrap();

        let raw_headers = batch
            .column_by_name("user_headers")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(!raw_headers.is_null(0));
        let registries = batch
            .column_by_name("schema_registry")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(registries.is_null(0));
    }
}
