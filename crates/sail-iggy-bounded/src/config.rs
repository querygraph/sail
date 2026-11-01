use std::fmt;
use std::time::Duration;

use datafusion::common::{Result, plan_datafusion_err, plan_err};

/// How a message's payload becomes a row, as in the sail-iggy reader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadFormat {
    /// One JSON object per message, decoded into the declared schema.
    Json,
    /// Broker metadata and the payload bytes.
    Raw,
    /// The archival envelope: every broker field, payload untouched.
    Envelope,
}

impl PayloadFormat {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "json" => Some(Self::Json),
            "raw" => Some(Self::Raw),
            "envelope" => Some(Self::Envelope),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Raw => "raw",
            Self::Envelope => "envelope",
        }
    }
}

/// What identifies a source and how to reach it. `Debug` and the descriptor
/// show the address only: the connection string can carry credentials, and
/// the descriptor is stored in run records.
#[derive(Clone)]
pub struct IggyBoundedConfig {
    pub url: String,
    pub stream: String,
    pub topic: String,
    pub format: PayloadFormat,
    /// The envelope's `source_id` column; required for that format.
    pub source_id: Option<String>,
    /// The longest one source call may take. The SDK reconnects without
    /// limit by default, so nothing else bounds a call to a dead broker.
    pub timeout: Duration,
    /// Messages asked for by one poll.
    pub page_size: u32,
}

impl IggyBoundedConfig {
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
    pub const DEFAULT_PAGE_SIZE: u32 = 1_000;

    pub fn new(url: &str, stream: &str, topic: &str, format: PayloadFormat) -> Self {
        Self {
            url: url.to_string(),
            stream: stream.to_string(),
            topic: topic.to_string(),
            format,
            source_id: None,
            timeout: Self::DEFAULT_TIMEOUT,
            page_size: Self::DEFAULT_PAGE_SIZE,
        }
    }

    /// A configuration from source options, whose names match the sail-iggy
    /// reader's and are case-insensitive as in Spark. An option that belongs
    /// to that reader's other modes is refused rather than ignored: a
    /// bounded read decides its own positions and never commits.
    pub fn from_options(options: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        let (mut url, mut stream, mut topic) = (None, None, None);
        let mut config = Self::new("", "", "", PayloadFormat::Json);
        for (key, value) in options {
            match key.to_ascii_lowercase().as_str() {
                "url" => url = Some(value),
                "stream" => stream = Some(value),
                "topic" => topic = Some(value),
                "source_id" => config.source_id = Some(value),
                "payload_format" => {
                    config.format = PayloadFormat::parse(&value).ok_or_else(|| {
                        plan_datafusion_err!("unsupported Iggy payload format: {value}")
                    })?;
                }
                "batch_size" => {
                    config.page_size = value
                        .parse::<u32>()
                        .ok()
                        .filter(|size| *size > 0)
                        .ok_or_else(|| plan_datafusion_err!("invalid Iggy batch_size: {value}"))?;
                }
                "timeout_ms" => {
                    let millis = value
                        .parse::<u64>()
                        .ok()
                        .filter(|millis| *millis > 0)
                        .ok_or_else(|| plan_datafusion_err!("invalid Iggy timeout_ms: {value}"))?;
                    config.timeout = Duration::from_millis(millis);
                }
                other => return plan_err!("unsupported bounded Iggy option: {other}"),
            }
        }
        let required = |value: Option<String>, name: &str| {
            value
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| plan_datafusion_err!("Iggy reads require the {name} option"))
        };
        config.url = required(url, "url")?;
        config.stream = required(stream, "stream")?;
        config.topic = required(topic, "topic")?;
        Ok(config)
    }

    /// The scheme, host and port: no credentials, no client options. A
    /// rotated password must not change a source's identity either.
    pub(crate) fn address(&self) -> String {
        let (scheme, rest) = self.url.split_once("://").unwrap_or(("", &self.url));
        let authority = rest.split(['/', '?']).next().unwrap_or("");
        let host = authority.rsplit('@').next().unwrap_or("");
        if scheme.is_empty() {
            host.to_string()
        } else {
            format!("{scheme}://{host}")
        }
    }

    pub(crate) fn descriptor(&self) -> serde_json::Value {
        serde_json::json!({
            "address": self.address(),
            "stream": self.stream,
            "topic": self.topic,
            "format": self.format.name(),
        })
    }
}

impl fmt::Debug for IggyBoundedConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IggyBoundedConfig")
            .field("address", &self.address())
            .field("stream", &self.stream)
            .field("topic", &self.topic)
            .field("format", &self.format)
            .field("timeout", &self.timeout)
            .field("page_size", &self.page_size)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &str) -> IggyBoundedConfig {
        IggyBoundedConfig::new(url, "s", "t", PayloadFormat::Json)
    }

    fn options(items: &[(&str, &str)]) -> Result<IggyBoundedConfig> {
        IggyBoundedConfig::from_options(
            items
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string())),
        )
    }

    #[test]
    fn options_are_case_insensitive_and_keep_their_defaults() -> Result<()> {
        let config = options(&[("URL", "iggy://h:1"), ("Stream", "s"), ("TOPIC", "t")])?;
        assert_eq!(config.url, "iggy://h:1");
        assert_eq!((config.stream.as_str(), config.topic.as_str()), ("s", "t"));
        assert_eq!(config.format, PayloadFormat::Json);
        assert_eq!(config.timeout, IggyBoundedConfig::DEFAULT_TIMEOUT);
        let config = options(&[
            ("url", "iggy://h:1"),
            ("stream", "s"),
            ("topic", "t"),
            ("payload_format", "RAW"),
            ("batch_size", "7"),
            ("timeout_ms", "250"),
        ])?;
        assert_eq!(config.format, PayloadFormat::Raw);
        assert_eq!(config.page_size, 7);
        assert_eq!(config.timeout, Duration::from_millis(250));
        Ok(())
    }

    #[test]
    fn missing_invalid_and_unbounded_options_are_refused() {
        let base = [("url", "iggy://h:1"), ("stream", "s"), ("topic", "t")];
        for (extra, expected) in [
            (("auto_commit", "true"), "unsupported bounded Iggy option"),
            (
                ("polling_strategy", "next"),
                "unsupported bounded Iggy option",
            ),
            (("partition", "1"), "unsupported bounded Iggy option"),
            (
                ("payload_format", "avro"),
                "unsupported Iggy payload format",
            ),
            (("batch_size", "0"), "invalid Iggy batch_size"),
            (("timeout_ms", "0"), "invalid Iggy timeout_ms"),
        ] {
            let mut items = base.to_vec();
            items.push(extra);
            let error = options(&items)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert!(error.contains(expected), "{error}");
        }
        for present in [
            &[("stream", "s"), ("topic", "t")][..],
            &[("url", "u"), ("topic", "t")][..],
            &[("url", "u"), ("stream", "s"), ("topic", " ")][..],
        ] {
            assert!(options(present).is_err());
        }
    }

    #[test]
    fn the_address_drops_credentials_and_client_options() {
        for (url, expected) in [
            ("iggy://user:secret@host:8090", "iggy://host:8090"),
            (
                "iggy+tcp://u:p@s@host:8090?tls=true",
                "iggy+tcp://host:8090",
            ),
            ("iggy://host:8090/", "iggy://host:8090"),
            ("host:8090", "host:8090"),
        ] {
            assert_eq!(config(url).address(), expected);
        }
    }

    #[test]
    fn neither_debug_nor_the_descriptor_shows_the_password() {
        let config = config("iggy://user:secret@host:8090?token=hidden");
        for text in [format!("{config:?}"), config.descriptor().to_string()] {
            assert!(text.contains("host:8090"), "{text}");
            assert!(!text.contains("secret"), "{text}");
            assert!(!text.contains("hidden"), "{text}");
            assert!(!text.contains("user"), "{text}");
        }
    }

    #[test]
    fn a_rotated_password_keeps_the_source_identity() {
        assert_eq!(
            config("iggy://user:old@host:8090").descriptor(),
            config("iggy://user:new@host:8090").descriptor()
        );
    }
}
