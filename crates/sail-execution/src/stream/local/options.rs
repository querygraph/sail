use std::time::Duration;

use crate::driver::DriverOptions;
use crate::worker::WorkerOptions;

#[readonly::make]
pub struct LocalStreamManagerOptions {
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
}

impl From<&DriverOptions> for LocalStreamManagerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
        }
    }
}

#[cfg(test)]
impl LocalStreamManagerOptions {
    pub(crate) fn for_tests() -> Self {
        Self {
            task_stream_buffer: 4,
            task_stream_creation_timeout: Duration::from_secs(1),
        }
    }
}

impl From<&WorkerOptions> for LocalStreamManagerOptions {
    fn from(options: &WorkerOptions) -> Self {
        Self {
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
        }
    }
}
