//! The adapter against a real `iggy-server` at the pinned revision. Set
//! `SAIL_IGGY_SERVER_BINARY` to its path; without it each test says so and
//! passes, since the workspace does not build the broker.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use iggy::prelude::*;
use sail_common_datafusion::streaming::bounded::{
    BoundedSource, Cursor, PartitionId, Range, ScanError, SourceError,
};
use sail_iggy_bounded::{IggyBoundedConfig, IggyBoundedSource, PayloadFormat};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const STREAM: &str = "cdc";
const TOPIC: &str = "events";
const PASSWORD: &str = "sail-test-password";

struct Server {
    binary: PathBuf,
    data: TempDir,
    port: u16,
    envs: Vec<(String, String)>,
    child: Option<Child>,
}

impl Server {
    fn start(
        envs: &[(&str, &str)],
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(binary) = std::env::var_os("SAIL_IGGY_SERVER_BINARY") else {
            eprintln!("skipped: SAIL_IGGY_SERVER_BINARY is not set");
            return Ok(None);
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);
        let mut server = Self {
            binary: PathBuf::from(binary),
            data: TempDir::new()?,
            port,
            envs: envs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            child: None,
        };
        server.spawn()?;
        Ok(Some(server))
    }

    fn spawn(&mut self) -> TestResult {
        let path: &Path = self.data.path();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.join("server.log"))?;
        let child = Command::new(&self.binary)
            .current_dir(path)
            .env("IGGY_PATH", path.join("data"))
            .env("IGGY_TCP_ADDRESS", format!("127.0.0.1:{}", self.port))
            .env("IGGY_ROOT_USERNAME", "iggy")
            .env("IGGY_ROOT_PASSWORD", PASSWORD)
            .env("IGGY_HTTP_ENABLED", "false")
            .env("IGGY_QUIC_ENABLED", "false")
            .env("IGGY_WEBSOCKET_ENABLED", "false")
            .env("IGGY_SHARDING_CPU_ALLOCATION", "0..2")
            .env("IGGY_SHARDING_PIN_CORES", "false")
            .envs(self.envs.iter().map(|(k, v)| (k, v)))
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        self.child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(60);
        while std::net::TcpStream::connect(("127.0.0.1", self.port)).is_err() {
            if Instant::now() > deadline {
                return Err(format!("broker did not listen: {}", self.log()).into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.data.path().join("server.log")).unwrap_or_default()
    }

    fn url(&self) -> String {
        self.url_with(PASSWORD)
    }

    fn url_with(&self, password: &str) -> String {
        format!("iggy://iggy:{password}@127.0.0.1:{}", self.port)
    }

    fn kill(&mut self) -> TestResult {
        if let Some(mut child) = self.child.take() {
            child.kill()?;
            child.wait()?;
        }
        Ok(())
    }

    /// Stop the broker the way an operator would, so it flushes.
    fn stop(&mut self) -> TestResult {
        if let Some(mut child) = self.child.take() {
            Command::new("kill")
                .arg("-TERM")
                .arg(child.id().to_string())
                .status()?;
            let deadline = Instant::now() + Duration::from_secs(30);
            while child.try_wait()?.is_none() {
                if Instant::now() > deadline {
                    child.kill()?;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn client(server: &Server) -> Result<IggyClient, Box<dyn std::error::Error + Send + Sync>> {
    let client = IggyClient::from_connection_string(&server.url())?;
    client.connect().await?;
    Ok(client)
}

async fn create_topic(client: &IggyClient, options: TopicCreateOptions) -> TestResult {
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await?;
    client
        .create_topic(&Identifier::named(STREAM)?, TOPIC, &options)
        .await?;
    Ok(())
}

fn partitions(count: u32) -> TopicCreateOptions {
    TopicCreateOptions {
        partitions_count: Some(count),
        ..TopicCreateOptions::default()
    }
}

async fn send(client: &IggyClient, partition: u32, ids: std::ops::Range<i64>) -> TestResult {
    let mut messages = ids
        .map(|id| IggyMessage::from_str(&format!(r#"{{"id":{id},"pad":"{}"}}"#, "x".repeat(900))))
        .collect::<Result<Vec<_>, _>>()?;
    client
        .send_messages(
            &Identifier::named(STREAM)?,
            &Identifier::named(TOPIC)?,
            &Partitioning::partition_id(partition),
            &mut messages,
        )
        .await?;
    Ok(())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn source(server: &Server) -> Result<IggyBoundedSource, datafusion::error::DataFusionError> {
    source_at(&server.url(), Duration::from_secs(30))
}

fn source_at(
    url: &str,
    timeout: Duration,
) -> Result<IggyBoundedSource, datafusion::error::DataFusionError> {
    let mut config = IggyBoundedConfig::new(url, STREAM, TOPIC, PayloadFormat::Json);
    config.timeout = timeout;
    IggyBoundedSource::try_new(config, schema())
}

fn partition(id: u32) -> PartitionId {
    PartitionId(id.to_string())
}

fn cursor(offset: u64) -> Cursor {
    Cursor(format!("offset:{offset}"))
}

async fn ids(
    source: &IggyBoundedSource,
    range: &Range,
) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
    let state = SessionStateBuilder::new().build();
    let plan = source.scan(range)?.scan(&state, None, &[], None).await?;
    let mut ids = Vec::new();
    for batch in collect(plan, state.task_ctx()).await? {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        ids.extend(column.iter().flatten());
    }
    Ok(ids)
}

fn refusal<T>(result: Result<T, SourceError>) -> Option<ScanError> {
    match result {
        Err(SourceError::Refused(error)) => Some(error),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_frontier_and_an_exact_range_on_a_real_broker() -> TestResult {
    let Some(server) = Server::start(&[])? else {
        return Ok(());
    };
    let client = client(&server).await?;
    create_topic(&client, partitions(2)).await?;
    send(&client, 0, 0..5).await?;
    let source = source(&server)?;
    let descriptor = source.describe()?;
    assert_eq!(
        descriptor
            .partitions
            .iter()
            .map(|x| x.id.clone())
            .collect::<Vec<_>>(),
        vec![partition(0), partition(1)]
    );
    assert!(!descriptor.descriptor.to_string().contains(PASSWORD));
    assert_eq!(source.earliest(&partition(0))?, cursor(0));
    assert_eq!(source.frontier(&partition(0))?, cursor(5));
    // The partition nothing was sent to.
    assert_eq!(source.earliest(&partition(1))?, cursor(0));
    assert_eq!(source.frontier(&partition(1))?, cursor(0));
    let range = Range {
        partition: partition(0),
        incarnation: descriptor.partitions[0].incarnation.clone(),
        start: cursor(1),
        end: cursor(4),
    };
    assert_eq!(ids(&source, &range).await?, vec![1, 2, 3]);
    // A read stores no position: it can be issued again unchanged.
    assert_eq!(ids(&source, &range).await?, vec![1, 2, 3]);
    let offset = client
        .get_consumer_offset(
            &Consumer::new(Identifier::named("sail-bounded")?),
            &Identifier::named(STREAM)?,
            &Identifier::named(TOPIC)?,
            Some(0),
        )
        .await?;
    assert_eq!(offset.map(|x| x.stored_offset), None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_message_past_the_captured_frontier_stays_out_of_the_range() -> TestResult {
    let Some(server) = Server::start(&[])? else {
        return Ok(());
    };
    let client = client(&server).await?;
    create_topic(&client, partitions(1)).await?;
    send(&client, 0, 0..3).await?;
    let source = source(&server)?;
    let frontier = source.frontier(&partition(0))?;
    let incarnation = source.describe()?.partitions[0].incarnation.clone();
    send(&client, 0, 3..6).await?;
    let range = Range {
        partition: partition(0),
        incarnation,
        start: cursor(0),
        end: frontier,
    };
    assert_eq!(ids(&source, &range).await?, vec![0, 1, 2]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn retention_fails_a_range_closed_and_moves_earliest_forward() -> TestResult {
    // The settings the pinned broker's own cleanup test uses.
    let Some(server) = Server::start(&[
        ("IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED", "true"),
        ("IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL", "100ms"),
        ("IGGY_MESSAGE_BUS_MAX_MESSAGE_SIZE", "2097152"),
    ])?
    else {
        return Ok(());
    };
    let client = client(&server).await?;
    create_topic(
        &client,
        TopicCreateOptions {
            partitions_count: Some(1),
            max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(
                4 * 1024 * 1024_u64,
            ))),
            segment_size: Some(IggyByteSize::from(1024 * 1024_u64)),
            ..TopicCreateOptions::default()
        },
    )
    .await?;
    for batch in 0..120_i64 {
        send(&client, 0, batch * 100..(batch + 1) * 100).await?;
    }
    let source = source(&server)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let first = loop {
        let earliest = source.earliest(&partition(0))?;
        if earliest != cursor(0) {
            break earliest;
        }
        if Instant::now() > deadline {
            return Err("retention did not remove offset 0".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let incarnation = source.describe()?.partitions[0].incarnation.clone();
    let range = Range {
        partition: partition(0),
        incarnation: incarnation.clone(),
        start: cursor(0),
        end: cursor(10),
    };
    let Some(ScanError::RetentionGap {
        partition: refused,
        expected,
        first_retained,
    }) = refusal(source.scan(&range))
    else {
        return Err("expected a retention gap".into());
    };
    assert_eq!(refused, partition(0));
    assert_eq!(expected, cursor(0));
    let (Some(seen), Some(earlier)) = (
        first_retained.0.strip_prefix("offset:"),
        first.0.strip_prefix("offset:"),
    ) else {
        return Err("expected minted cursors".into());
    };
    // Retention may have moved on between the two reads, never back.
    assert!(seen.parse::<u64>()? >= earlier.parse::<u64>()?);
    // A new run from `earliest` reads what is left.
    let from = source.earliest(&partition(0))?;
    let start = from.0.trim_start_matches("offset:").parse::<u64>()?;
    let range = Range {
        partition: partition(0),
        incarnation,
        start: from,
        end: cursor(start + 5),
    };
    let expected: Vec<i64> = (0..5)
        .map(|x| i64::try_from(start).unwrap_or(0) + x)
        .collect();
    assert_eq!(ids(&source, &range).await?, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_partition_is_refused_whether_or_not_it_has_refilled() -> TestResult {
    let Some(server) = Server::start(&[])? else {
        return Ok(());
    };
    let client = client(&server).await?;
    create_topic(&client, partitions(1)).await?;
    send(&client, 0, 0..4).await?;
    let source = source(&server)?;
    let old = source.describe()?.partitions[0].incarnation.clone();
    let range = Range {
        partition: partition(0),
        incarnation: old.clone(),
        start: cursor(0),
        end: cursor(2),
    };
    assert_eq!(ids(&source, &range).await?, vec![0, 1]);
    let (stream, topic) = (Identifier::named(STREAM)?, Identifier::named(TOPIC)?);
    client.delete_partitions(&stream, &topic, 1).await?;
    client.create_partitions(&stream, &topic, 1).await?;
    let new = source.describe()?.partitions[0].incarnation.clone();
    assert_ne!(old, new);
    let expected = ScanError::IncarnationMismatch {
        partition: partition(0),
        expected: old,
        actual: new,
    };
    // Empty: nothing to read, and still not a gap.
    assert_eq!(refusal(source.scan(&range)), Some(expected.clone()));
    // Refilled: the same offsets hold other rows.
    send(&client, 0, 100..104).await?;
    assert_eq!(refusal(source.scan(&range)), Some(expected));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_broker_restart_changes_nothing_a_range_depends_on() -> TestResult {
    let Some(mut server) = Server::start(&[])? else {
        return Ok(());
    };
    let client = client(&server).await?;
    create_topic(&client, partitions(1)).await?;
    send(&client, 0, 0..6).await?;
    let source = source(&server)?;
    let before = source.describe()?;
    let frontier = source.frontier(&partition(0))?;
    let range = Range {
        partition: partition(0),
        incarnation: before.partitions[0].incarnation.clone(),
        start: cursor(2),
        end: cursor(6),
    };
    assert_eq!(ids(&source, &range).await?, vec![2, 3, 4, 5]);
    drop(client);
    server.stop()?;
    server.spawn()?;
    // The same source object, reconnected by the SDK.
    assert_eq!(source.describe()?, before);
    assert_eq!(source.frontier(&partition(0))?, frontier);
    assert_eq!(ids(&source, &range).await?, vec![2, 3, 4, 5]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_broker_is_unavailable_within_the_deadline() -> TestResult {
    let Some(mut server) = Server::start(&[])? else {
        return Ok(());
    };
    let client = client(&server).await?;
    create_topic(&client, partitions(1)).await?;
    send(&client, 0, 0..3).await?;
    let source = source_at(&server.url(), Duration::from_secs(2))?;
    let incarnation = source.describe()?.partitions[0].incarnation.clone();
    server.kill()?;
    let started = Instant::now();
    let result = source.frontier(&partition(0));
    assert!(
        matches!(result, Err(SourceError::Unavailable { .. })),
        "{result:?}"
    );
    let range = Range {
        partition: partition(0),
        incarnation,
        start: cursor(0),
        end: cursor(3),
    };
    let result = source.scan(&range);
    assert!(
        matches!(&result, Err(error) if error.retryable()),
        "{:?}",
        result.err()
    );
    assert!(started.elapsed() < Duration::from_secs(20));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_credentials_a_missing_topic_and_a_missing_partition_stay_distinct() -> TestResult {
    let Some(server) = Server::start(&[])? else {
        return Ok(());
    };
    let client = client(&server).await?;
    create_topic(&client, partitions(1)).await?;
    send(&client, 0, 0..2).await?;
    let denied = source_at(&server.url_with("wrong"), Duration::from_secs(10))?.describe();
    assert!(
        matches!(denied, Err(SourceError::Denied { .. })),
        "{denied:?}"
    );

    let mut config = IggyBoundedConfig::new(&server.url(), STREAM, "absent", PayloadFormat::Json);
    config.timeout = Duration::from_secs(10);
    let missing = IggyBoundedSource::try_new(config, schema())?.describe();
    assert!(
        matches!(missing, Err(SourceError::NotFound { .. })),
        "{missing:?}"
    );

    let source = source(&server)?;
    assert_eq!(
        refusal(source.frontier(&partition(5))),
        Some(ScanError::UnknownPartition {
            partition: partition(5)
        })
    );
    let incarnation = source.describe()?.partitions[0].incarnation.clone();
    let range = Range {
        partition: partition(5),
        incarnation,
        start: cursor(0),
        end: cursor(1),
    };
    assert_eq!(
        refusal(source.scan(&range)),
        Some(ScanError::UnknownPartition {
            partition: partition(5)
        })
    );
    Ok(())
}
