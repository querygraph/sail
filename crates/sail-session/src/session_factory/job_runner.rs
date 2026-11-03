use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use sail_common::actor::ActorSystem;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::job::JobRunner;
use sail_execution::DriverId;
use sail_execution::driver::{DriverComponents, DriverHandle, DriverOptions};
use sail_execution::error::ExecutionResult;
use sail_execution::job_runner::{ClusterJobRunner, LocalJobRunner};
use sail_execution::worker_manager::{
    KubernetesWorkerManager, KubernetesWorkerManagerOptions, LocalWorkerManager,
};
use sail_telemetry::events::SystemEventReporter;

use crate::error::{SessionError, SessionResult};
use crate::runtime::MemoryResourceDomain;
use crate::session_factory::{SessionFactory, WorkerSessionFactory};

/// The asynchronous work needed before a new session can run.
pub(crate) type SessionActivation = BoxFuture<'static, ExecutionResult<()>>;

pub struct SessionJobRunner {
    runner: Box<dyn JobRunner>,
    driver: Option<DriverHandle>,
    activation: Option<SessionActivation>,
}

impl SessionJobRunner {
    fn local(runner: LocalJobRunner) -> Self {
        Self {
            runner: Box::new(runner),
            driver: None,
            activation: None,
        }
    }

    fn cluster(runner: ClusterJobRunner) -> Self {
        let driver = runner.driver();
        let activation = {
            let driver = driver.clone();
            async move { driver.activate().await }.boxed()
        };
        Self {
            runner: Box::new(runner),
            driver: Some(driver),
            activation: Some(activation),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_activation(
        runner: Box<dyn JobRunner>,
        activation: Option<SessionActivation>,
    ) -> Self {
        Self {
            runner,
            driver: None,
            activation,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Box<dyn JobRunner>,
        Option<DriverHandle>,
        Option<SessionActivation>,
    ) {
        (self.runner, self.driver, self.activation)
    }
}

pub struct SessionJobRunnerInfo {
    pub resource_domain: Option<MemoryResourceDomain>,
    pub session_id: String,
    pub driver_id: DriverId,
    pub driver_server_port: Option<u16>,
    pub event_reporter: SystemEventReporter,
}

pub trait SessionJobRunnerFactory: Send {
    fn create(
        &mut self,
        system: &mut ActorSystem,
        info: SessionJobRunnerInfo,
    ) -> SessionResult<SessionJobRunner>;
}

pub struct ServerSessionJobRunnerFactory {
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
}

impl ServerSessionJobRunnerFactory {
    pub fn new(config: Arc<AppConfig>, runtime: RuntimeHandle) -> Self {
        Self { config, runtime }
    }

    fn create_cluster_runner(
        &self,
        system: &mut ActorSystem,
        info: SessionJobRunnerInfo,
        worker_manager: Box<dyn sail_execution::worker_manager::WorkerManager>,
    ) -> SessionResult<SessionJobRunner> {
        let Some(port) = info.driver_server_port else {
            return Err(SessionError::internal("driver gateway is not available"));
        };
        let options = DriverOptions::try_new(
            &self.config,
            self.runtime.clone(),
            info.session_id,
            info.driver_id,
            port,
        )?;
        let components = DriverComponents {
            worker_manager,
            event_reporter: info.event_reporter,
        };
        Ok(SessionJobRunner::cluster(ClusterJobRunner::new(
            system, options, components,
        )))
    }
}

impl SessionJobRunnerFactory for ServerSessionJobRunnerFactory {
    fn create(
        &mut self,
        system: &mut ActorSystem,
        info: SessionJobRunnerInfo,
    ) -> SessionResult<SessionJobRunner> {
        match self.config.mode {
            ExecutionMode::Local => Ok(SessionJobRunner::local(LocalJobRunner::new(
                info.session_id,
            ))),
            ExecutionMode::LocalCluster => {
                let worker_session =
                    WorkerSessionFactory::new(self.config.clone(), self.runtime.clone())
                        .with_resource_domain(info.resource_domain.clone())
                        .create(())?;
                self.create_cluster_runner(
                    system,
                    info,
                    Box::new(LocalWorkerManager::new(
                        self.runtime.clone(),
                        worker_session,
                    )),
                )
            }
            ExecutionMode::KubernetesCluster => {
                let options = KubernetesWorkerManagerOptions {
                    image: self.config.kubernetes.image.clone(),
                    image_pull_policy: self.config.kubernetes.image_pull_policy.clone(),
                    namespace: self.config.kubernetes.namespace.clone(),
                    driver_pod_name: self.config.kubernetes.driver_pod_name.clone(),
                    worker_pod_name_prefix: self.config.kubernetes.worker_pod_name_prefix.clone(),
                    worker_service_account_name: self
                        .config
                        .kubernetes
                        .worker_service_account_name
                        .clone(),
                    worker_pod_template: self.config.kubernetes.worker_pod_template.clone(),
                };
                self.create_cluster_runner(
                    system,
                    info,
                    Box::new(KubernetesWorkerManager::new(options)),
                )
            }
        }
    }
}
