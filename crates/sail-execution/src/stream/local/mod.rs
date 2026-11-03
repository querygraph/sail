mod core;
mod memory;
mod options;

use std::collections::HashMap;

use datafusion::arrow::array::RecordBatch;
pub use options::LocalStreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::id::TaskStreamKey;
use crate::stream::error::TaskStreamResult;

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
    streams: HashMap<TaskStreamKey, LocalStreamState>,
}

pub enum LocalStreamState {
    Pending {
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    },
    Created {
        stream: memory::MemoryStream,
        /// Cancelled when the producing task is stopped or the runner shuts
        /// down. Every subscriber ends with the cancel cause at that moment,
        /// whether or not the producer's future has released its sink yet.
        ended: CancellationToken,
    },
    Failed {
        cause: CommonErrorCause,
    },
}
