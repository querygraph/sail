use std::error::Error;

use tokio::task::JoinError;

use super::*;
use crate::error::{CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor};

type TestResult = std::result::Result<(), Box<dyn Error>>;
struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

fn join_error(error: &DataFusionError) -> Option<&JoinError> {
    match error {
        DataFusionError::External(error) => error.downcast_ref(),
        DataFusionError::Collection(errors) => errors.iter().find_map(join_error),
        _ => None,
    }
}

#[tokio::test]
async fn mutation_interruption_keeps_typed_panic_and_nonretryable_uncertainty() -> TestResult {
    for mutation in [false, true] {
        let scope = TaskResourceScope::new(Handle::current());
        let factory = |_| async {
            // The registered mutation may have published before any arbitrary
            // panic. A typed JoinError is evidence of interruption, not rollback.
            std::panic::resume_unwind(Box::new("injected supervisor panic"));
            #[expect(unreachable_code)]
            Ok(())
        };
        if mutation {
            scope.spawn_mutation(factory)?;
        } else {
            scope.spawn(factory)?;
        }
        let report = scope.drain().await?;
        assert!(!report.is_clean());
        assert_eq!(report.registered, 1);
        assert!(join_error(&report.failures[0].error).is_some_and(JoinError::is_panic));
        for error in report.errors() {
            let cause = CommonErrorCause::new::<NoPython>(&error);
            assert_eq!(!cause.is_retryable(), mutation);
            assert_eq!(
                matches!(cause, CommonErrorCause::CommitStateUnknown(_)),
                mutation
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn returned_mutation_error_is_not_reclassified_as_interruption() -> TestResult {
    let scope = TaskResourceScope::new(Handle::current());
    scope.spawn_mutation(|_| async {
        Err(DataFusionError::Execution(
            "provider abort confirmed after part failure".into(),
        ))
    })?;
    let report = scope.drain().await?;
    assert!(!report.is_clean());
    assert_eq!(report.failures.len(), 1);
    assert!(
        matches!(&report.failures[0].error,DataFusionError::Execution(message) if message=="provider abort confirmed after part failure")
    );
    assert!(CommonErrorCause::new::<NoPython>(&report.failures[0].error).is_retryable());
    Ok(())
}

#[tokio::test]
async fn cooperative_mutation_cancellation_can_finish_cleanly() -> TestResult {
    let scope = TaskResourceScope::new(Handle::current());
    scope.spawn_mutation(|mut canceled| async move {
        canceled
            .wait_for(|value| *value)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        // A real supervisor would confirm provider settlement before returning.
        Ok(())
    })?;
    scope.cancel()?;
    let report = scope.drain().await?;
    assert!(report.is_clean());
    assert_eq!(report.registered, 1);
    Ok(())
}

#[tokio::test]
async fn interrupted_result_observer_classifies_only_admitted_mutations() -> TestResult {
    for mutation in [false, true] {
        let scope = TaskResourceScope::new(Handle::current());
        let factory = |_| async {
            std::panic::resume_unwind(Box::new("lost result"));
            #[expect(unreachable_code)]
            Ok(())
        };
        let result = if mutation {
            scope.run_mutation(factory).await
        } else {
            scope.run(factory).await
        };
        let observed = result.err().ok_or("expected interrupted result observer")?;
        let cause = CommonErrorCause::new::<NoPython>(&observed);
        assert_eq!(!cause.is_retryable(), mutation);
        assert_eq!(
            matches!(cause, CommonErrorCause::CommitStateUnknown(_)),
            mutation
        );
        let errors: &[DataFusionError] = match &observed {
            DataFusionError::Collection(errors) => errors,
            error => std::slice::from_ref(error),
        };
        assert!(errors.iter().any(|error| matches!(error,
            DataFusionError::Context(_, source) if matches!(source.as_ref(),
                DataFusionError::External(source) if source.downcast_ref::<oneshot::error::RecvError>().is_some()
            )
        )));
        let report = scope.drain().await?;
        assert!(!report.is_clean());
        assert!(join_error(&report.failures[0].error).is_some_and(JoinError::is_panic));
    }
    Ok(())
}
