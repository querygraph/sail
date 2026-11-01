use std::future::Future;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use sail_common::actor::ActorSystem;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common::server::{ServerBuilder, ServerBuilderOptions};
use sail_session::session_manager::serve_until_stopped;
use tokio::net::TcpListener;

use crate::service::SailFlightSqlService;
use crate::session::create_flight_session_manager;

pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let mut system = ActorSystem::new();
    let session_manager = create_flight_session_manager(config, runtime, &mut system).await?;
    let service = SailFlightSqlService::new(session_manager.clone());
    let flight_service = FlightServiceServer::new(service);
    let builder = ServerBuilder::new("flight_sql", ServerBuilderOptions::default())
        .add_service(flight_service, None)
        .await;
    serve_until_stopped(&session_manager, &mut system, signal, |drain| async move {
        builder
            .serve(listener, drain)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
