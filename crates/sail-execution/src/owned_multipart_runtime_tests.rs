use tokio::task::JoinError;

use super::*;

fn join_error(error: &DataFusionError) -> Option<&JoinError> {
    match error {
        DataFusionError::External(error) => error.downcast_ref(),
        DataFusionError::Shared(error) => join_error(error),
        DataFusionError::Context(_, error) => join_error(error),
        DataFusionError::Collection(errors) => errors.iter().find_map(join_error),
        _ => None,
    }
}

#[test]
fn runtime_loss_after_actual_publication_retains_nonretryable_uncertainty() -> TestResult {
    let caller = tokio::runtime::Runtime::new()?;
    let io = tokio::runtime::Runtime::new()?;
    let scope = TaskResourceScope::new(io.handle().clone());
    let state = State::new();
    let provider = state.clone();
    let (after_publish, published, _release) = Gate::new();
    let outcome = caller.block_on(async {
        let mut upload = create(
            &scope,
            move || async move {
                let mut upload = Upload::new(provider).await?;
                upload.after_publish = Some(after_publish);
                Ok(Box::new(upload) as Box<dyn MultipartUpload>)
            },
            capacity(),
        )
        .await?;
        upload
            .put_part("published-before-runtime-loss".into())?
            .await?;
        let outcome = upload.complete()?;
        timeout(Duration::from_secs(5), published).await??;
        assert_eq!(
            state.store.get(&state.path).await?.bytes().await?,
            "published-before-runtime-loss"
        );
        Ok::<_, Box<dyn Error>>(outcome)
    })?;
    // The response is held after a real InMemory commit. Shutting down the
    // provider runtime loses its supervisor while keeping the scope/drainer
    // alive on a distinct runtime. A failed join cannot prove commit absence.
    io.shutdown_timeout(Duration::from_secs(5));
    let report = caller.block_on(async {
        let interrupted = outcome.await.err().ok_or("expected interrupted observer")?;
        assert!(matches!(interrupted, MultipartError::Interrupted));
        let mapped = execution_error(interrupted);
        let cause = CommonErrorCause::new::<NoPython>(&mapped);
        assert!(
            !cause.is_retryable(),
            "mutation observer was retryable before drainage"
        );
        assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
        Ok::<_, Box<dyn Error>>(timeout(Duration::from_secs(5), scope.drain()).await??)
    })?;
    assert!(!report.is_clean());
    assert_eq!(report.registered, 1);
    assert_eq!(report.failures.len(), 1);
    assert!(join_error(&report.failures[0].error).is_some_and(JoinError::is_cancelled));
    for error in report.errors() {
        let cause = CommonErrorCause::new::<NoPython>(&error);
        assert!(!cause.is_retryable());
        assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
    }
    caller.block_on(async {
        assert_eq!(
            state.store.get(&state.path).await?.bytes().await?,
            "published-before-runtime-loss"
        );
        assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
        Ok::<_, Box<dyn Error>>(())
    })?;
    Ok(())
}
