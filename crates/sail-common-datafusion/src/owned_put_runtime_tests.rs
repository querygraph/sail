use std::error::Error;

use store::PublishedResponse;

use super::*;
use crate::error::{CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor};

struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

#[test]
fn published_put_losing_its_runtime_keeps_nonretryable_drain_failure() -> TestResult {
    let caller = tokio::runtime::Runtime::new()?;
    let io = tokio::runtime::Runtime::new()?;
    let scope = Arc::new(TaskResourceScope::new(io.handle().clone()));
    let (entered, published) = oneshot::channel();
    let (_hold_response, release) = oneshot::channel();
    let store = Arc::new(HeldStore {
        inner: Arc::new(InMemory::new()),
        calls: AtomicUsize::new(0),
        fail: false,
        entered: Mutex::new(None),
        release: Mutex::new(None),
        after_publish: Mutex::new(Some(PublishedResponse { entered, release })),
    });
    let path = Path::from("published-put");
    let observer = {
        let scope = scope.clone();
        let store = store.clone();
        let path = path.clone();
        caller.spawn(async move {
            put(
                &scope,
                store,
                path,
                "published".into(),
                PutOptions::default(),
            )
            .await
        })
    };
    caller.block_on(async {
        timeout(Duration::from_secs(5), published).await??;
        assert_eq!(store.inner.get(&path).await?.bytes().await?, "published");
        Ok::<_, Box<dyn Error>>(())
    })?;
    io.shutdown_timeout(Duration::from_secs(5));
    caller.block_on(async {
        let observed = timeout(Duration::from_secs(5), observer).await??
            .err().ok_or("expected interrupted observer")?;
        let cause = CommonErrorCause::new::<NoPython>(&observed);
        assert!(!cause.is_retryable(), "mutation observer was retryable before drainage");
        assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
        let report = timeout(Duration::from_secs(5), scope.drain()).await??;
        assert!(!report.is_clean()); assert_eq!(report.registered, 1);
        assert_eq!(report.failures.len(), 1);
        let DataFusionError::Collection(errors) = &report.failures[0].error else { return Err("mutation uncertainty collection lost".into()); };
        assert!(errors.iter().any(|error| matches!(error, DataFusionError::External(source) if source.downcast_ref::<tokio::task::JoinError>().is_some_and(tokio::task::JoinError::is_cancelled))));
        for error in report.errors() {
            let cause = CommonErrorCause::new::<NoPython>(&error);
            assert!(!cause.is_retryable());
            assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
        }
        assert_eq!(store.inner.get(&path).await?.bytes().await?, "published");
        Ok::<_, Box<dyn Error>>(())
    })?;
    Ok(())
}
