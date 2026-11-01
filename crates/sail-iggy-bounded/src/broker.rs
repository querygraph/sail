//! The two things a bounded read needs from a broker, so the read rules can
//! be tested against every reply a broker could give.

use std::fmt;
use std::str::FromStr;

use async_trait::async_trait;
use iggy::prelude::{
    Client, Consumer, Identifier, IggyClient, IggyError, IggyMessage, MessageClient,
    PollingStrategy, TopicClient,
};
use sail_common_datafusion::streaming::bounded::{PartitionId, ScanError, SourceError};
use tokio::sync::OnceCell;

use crate::config::IggyBoundedConfig;

/// The name polls are made under. Nothing is stored for it.
const CONSUMER: &str = "sail-bounded";

/// A partition as the broker reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartitionState {
    pub id: u32,
    pub incarnation: u64,
    pub current_offset: u64,
    pub messages_count: u64,
}

impl PartitionState {
    /// The first offset past what can be read. `current_offset` names the
    /// last message, so an empty partition has to be told apart from one
    /// holding only offset zero. A partition that lost everything to
    /// retention at offset zero reads as empty too; the broker reports the
    /// same two numbers for both.
    pub fn next_offset(&self) -> u64 {
        if self.messages_count == 0 && self.current_offset == 0 {
            0
        } else {
            self.current_offset.saturating_add(1)
        }
    }
}

/// One poll's reply.
pub(crate) struct Page {
    pub incarnation: u64,
    pub messages: Vec<IggyMessage>,
}

#[async_trait]
pub(crate) trait Broker: Send + Sync + fmt::Debug + 'static {
    /// The topic's partitions now.
    async fn partitions(&self) -> Result<Vec<PartitionState>, SourceError>;

    /// Up to `count` messages from `offset`, without acknowledging them.
    async fn poll(&self, partition: u32, offset: u64, count: u32) -> Result<Page, SourceError>;
}

/// [`Broker`] over the Iggy client, connected on first use.
pub(crate) struct SdkBroker {
    config: IggyBoundedConfig,
    client: OnceCell<IggyClient>,
}

impl SdkBroker {
    pub fn new(config: IggyBoundedConfig) -> Self {
        Self {
            config,
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&IggyClient, SourceError> {
        self.client
            .get_or_try_init(|| async {
                let client = IggyClient::from_connection_string(&self.config.url)
                    .map_err(|e| self.classify(e, None))?;
                client.connect().await.map_err(|e| self.classify(e, None))?;
                Ok(client)
            })
            .await
    }

    fn identifiers(&self) -> Result<(Identifier, Identifier), SourceError> {
        let identifier = |name: &str| {
            Identifier::from_str(name).map_err(|e| SourceError::NotFound {
                object: name.to_string(),
                cause: Box::new(e),
            })
        };
        Ok((
            identifier(&self.config.stream)?,
            identifier(&self.config.topic)?,
        ))
    }

    fn object(&self) -> String {
        format!("{}/{}", self.config.stream, self.config.topic)
    }

    /// A read mutates nothing, so a transport failure may be retried. What
    /// this does not recognise is not offered for retry.
    fn classify(&self, error: IggyError, partition: Option<u32>) -> SourceError {
        let partition_id = partition.map(|id| PartitionId(id.to_string()));
        match error {
            IggyError::Disconnected
            | IggyError::CannotEstablishConnection
            | IggyError::NotConnected
            | IggyError::ConnectionClosed
            | IggyError::StaleClient
            | IggyError::EmptyResponse => SourceError::Unavailable {
                partition: partition_id,
                cause: Box::new(error),
            },
            IggyError::Unauthenticated
            | IggyError::Unauthorized
            | IggyError::InvalidCredentials => SourceError::Denied {
                cause: Box::new(error),
            },
            IggyError::StreamIdNotFound(..) | IggyError::TopicIdNotFound(..) => {
                SourceError::NotFound {
                    object: self.object(),
                    cause: Box::new(error),
                }
            }
            IggyError::PartitionNotFound(..) => match partition_id {
                Some(partition) => ScanError::UnknownPartition { partition }.into(),
                None => SourceError::Internal {
                    cause: Box::new(error),
                },
            },
            other => SourceError::Internal {
                cause: Box::new(other),
            },
        }
    }
}

impl fmt::Debug for SdkBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SdkBroker")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Broker for SdkBroker {
    async fn partitions(&self) -> Result<Vec<PartitionState>, SourceError> {
        let client = self.client().await?;
        let (stream, topic) = self.identifiers()?;
        let details = client
            .get_topic(&stream, &topic)
            .await
            .map_err(|e| self.classify(e, None))?
            .ok_or_else(|| SourceError::NotFound {
                object: self.object(),
                cause: "the broker does not have this topic".into(),
            })?;
        Ok(details
            .partitions
            .iter()
            .map(|partition| PartitionState {
                id: partition.id,
                incarnation: partition.context.incarnation,
                current_offset: partition.current_offset,
                messages_count: partition.messages_count,
            })
            .collect())
    }

    async fn poll(&self, partition: u32, offset: u64, count: u32) -> Result<Page, SourceError> {
        let client = self.client().await?;
        let (stream, topic) = self.identifiers()?;
        let consumer = Consumer::new(
            Identifier::named(CONSUMER)
                .map_err(|e| SourceError::Internal { cause: Box::new(e) })?,
        );
        let strategy = PollingStrategy::offset(offset);
        let poll = || {
            client.poll_messages(
                &stream,
                &topic,
                Some(partition),
                &consumer,
                &strategy,
                count,
                false,
            )
        };
        // The client caches a route per partition. After a partition is
        // recreated the first poll over the old route is refused and drops
        // it, and the next one reports the partition as it now is, which the
        // caller checks against the range.
        let polled = match poll().await {
            Err(IggyError::HistoryUnavailable) => poll().await,
            result => result,
        }
        .map_err(|e| self.classify(e, Some(partition)))?;
        Ok(Page {
            incarnation: polled.context.incarnation,
            messages: polled.messages,
        })
    }
}
