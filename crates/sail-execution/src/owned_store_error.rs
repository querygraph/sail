//! Project provider condition variants without losing the retained typed cause.
use datafusion::common::DataFusionError;
use sail_object_store::owned_multipart::{MultipartError, Stage};

fn provider(error: &DataFusionError) -> Option<&object_store::Error> {
    match error {
        DataFusionError::ObjectStore(error) => Some(error),
        DataFusionError::Shared(error) => provider(error),
        DataFusionError::Context(_, error) => provider(error),
        DataFusionError::External(error) => {
            let causes = error.downcast_ref::<MultipartError>()?.causes();
            match causes {
                [cause] if cause.stage != Stage::Complete => Some(&cause.error),
                _ => None,
            }
        }
        // In particular, do not discard uncertainty or sibling failures from
        // a Collection just to recover one provider's outer condition variant.
        _ => None,
    }
}

pub(super) fn store_error(error: DataFusionError) -> object_store::Error {
    use object_store::Error;
    match provider(&error) {
        Some(Error::AlreadyExists { path, .. }) => Error::AlreadyExists {
            path: path.clone(),
            source: Box::new(error),
        },
        Some(Error::Precondition { path, .. }) => Error::Precondition {
            path: path.clone(),
            source: Box::new(error),
        },
        Some(Error::NotFound { path, .. }) => Error::NotFound {
            path: path.clone(),
            source: Box::new(error),
        },
        Some(Error::NotModified { path, .. }) => Error::NotModified {
            path: path.clone(),
            source: Box::new(error),
        },
        Some(Error::PermissionDenied { path, .. }) => Error::PermissionDenied {
            path: path.clone(),
            source: Box::new(error),
        },
        Some(Error::Unauthenticated { path, .. }) => Error::Unauthenticated {
            path: path.clone(),
            source: Box::new(error),
        },
        Some(Error::NotSupported { .. }) => Error::NotSupported {
            source: Box::new(error),
        },
        Some(Error::NotImplemented {
            operation,
            implementer,
        }) => Error::NotImplemented {
            operation: operation.clone(),
            implementer: implementer.clone(),
        },
        Some(Error::UnknownConfigurationKey { store, key }) => Error::UnknownConfigurationKey {
            store,
            key: key.clone(),
        },
        // Non-cloneable path/join errors, multi-cause failures and interrupted
        // supervision retain the complete original typed tree in Generic.
        _ => Error::Generic {
            store: "OwnedObjectStore",
            source: Box::new(error),
        },
    }
}
