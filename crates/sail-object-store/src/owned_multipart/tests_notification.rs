use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};

use futures::task::{ArcWake, waker_ref};

use super::*;

#[derive(Default)]
struct CheckWake {
    admission: Mutex<Option<Arc<Mutex<Admission>>>>,
    blocked: AtomicBool,
}

impl ArcWake for CheckWake {
    fn wake_by_ref(this: &Arc<Self>) {
        let admission = this.admission.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(admission) = admission.as_ref()
            && admission.try_lock().is_err()
        {
            this.blocked.store(true, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn handle_notifications_happen_outside_admission_lock() -> TestResult {
    for abort in [false, true] {
        let (upload, _) = TestUpload::new().await?;
        let (supervisor, creation) = prepare(
            move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
            capacity(),
        );
        let (_cancel, canceled) = watch::channel(false);
        let mut owner = Box::pin(supervisor.run(canceled));
        let check = Arc::new(CheckWake::default());
        let waker = waker_ref(&check);
        let mut context = Context::from_waker(&waker);
        assert!(std::future::Future::poll(owner.as_mut(), &mut context).is_pending());
        let mut handle = creation.wait().await?;
        *check.admission.lock().map_err(|_| "waker lock")? = Some(handle.admission.clone());
        let part = handle.put_part("payload".into())?;
        assert!(
            !check.blocked.load(Ordering::SeqCst),
            "part notification held admission lock"
        );
        assert!(std::future::Future::poll(owner.as_mut(), &mut context).is_pending());
        part.await?;
        let result = if abort {
            handle.abort()?
        } else {
            handle.complete()?
        };
        assert!(
            !check.blocked.load(Ordering::SeqCst),
            "settlement notification held admission lock"
        );
        let Poll::Ready(result_owner) = std::future::Future::poll(owner.as_mut(), &mut context)
        else {
            return Err("fixture provider should settle immediately".into());
        };
        result_owner?;
        assert_eq!(matches!(result.await?, Disposition::Aborted), abort);
    }
    Ok(())
}
