use std::error::Error;
use std::fmt;
use std::future::Future;

use datafusion::common::DataFusionError;
use futures::FutureExt;
use futures::future::BoxFuture;
use sail_common::actor::ActorSystem;
use tokio::sync::oneshot;

use crate::error::SessionError;
use crate::session_manager::SessionManager;

/// Both the transport and the session manager failed.
#[derive(Debug)]
pub struct ServerStopError {
    pub transport: Box<dyn Error + Send + Sync>,
    pub shutdown: SessionError,
}

impl fmt::Display for ServerStopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "server transport failed: {}; session manager shutdown failed: ",
            self.transport
        )?;
        write_shutdown_error(f, &self.shutdown)
    }
}

impl Error for ServerStopError {}

/// Show every failure, since the display of a collection shows only the first.
fn write_shutdown_error(f: &mut fmt::Formatter<'_>, error: &SessionError) -> fmt::Result {
    match error {
        SessionError::DataFusionError(error) => write_data_fusion_error(f, error),
        error => write!(f, "{error}"),
    }
}

fn write_data_fusion_error(f: &mut fmt::Formatter<'_>, error: &DataFusionError) -> fmt::Result {
    match error {
        DataFusionError::Shared(error) => write_data_fusion_error(f, error),
        DataFusionError::Collection(errors) => {
            for (index, error) in errors.iter().enumerate() {
                if index > 0 {
                    f.write_str("; ")?;
                }
                write_data_fusion_error(f, error)?;
            }
            Ok(())
        }
        error => write!(f, "{error}"),
    }
}

/// Run a server until the signal fires or its transport exits, then stop everything.
///
/// The session manager starts shutting down at the same time as the transport starts draining,
/// and not after it. An active response can be waiting for a job, and that job only stops when
/// the session manager stops it, so waiting for the transport first would never finish.
/// The session manager also shuts down if the transport exits by itself.
///
/// The actor system is always joined, whatever fails, and neither failure hides the other.
pub async fn serve_until_stopped<S, T, F, E>(
    session_manager: &SessionManager,
    system: &mut ActorSystem,
    signal: S,
    transport: T,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    S: Future<Output = ()>,
    T: FnOnce(BoxFuture<'static, ()>) -> F,
    F: Future<Output = Result<(), E>>,
    E: Error + Send + Sync + 'static,
{
    let (drain, draining) = oneshot::channel::<()>();
    let transport = transport(
        async move {
            let _ = draining.await;
        }
        .boxed(),
    );
    tokio::pin!(signal, transport);
    let (transport, shutdown) = tokio::select! {
        result = &mut transport => (result, session_manager.shutdown().await),
        () = &mut signal => {
            let _ = drain.send(());
            tokio::join!(&mut transport, session_manager.shutdown())
        }
    };
    system.join().await;
    match (transport, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(transport), Ok(())) => Err(transport.into()),
        (Ok(()), Err(shutdown)) => Err(shutdown.into()),
        (Err(transport), Err(shutdown)) => Err(Box::new(ServerStopError {
            transport: transport.into(),
            shutdown,
        })),
    }
}
