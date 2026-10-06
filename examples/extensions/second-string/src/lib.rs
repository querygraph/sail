//! Spark Second String native functions, exported via DataFusion FFI.
mod function;
mod matrix;
mod options;
mod phonetic;
mod token;
mod udf;

use datafusion_expr::ScalarUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::sync::Arc;

#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct NativeScalarUdf {
    inner: Arc<ScalarUDF>,
}

#[pymethods]
impl NativeScalarUdf {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> Vec<String> {
        Vec::new()
    }
    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        PyCapsule::new_with_value(
            py,
            FFI_ScalarUDF::from(Arc::clone(&self.inner)),
            c"datafusion_scalar_udf",
        )
    }
}

#[pyclass]
struct BoundSecondString {
    #[pyo3(get)]
    session_id: String,
    functions: Vec<NativeScalarUdf>,
}

#[pymethods]
impl BoundSecondString {
    #[new]
    fn new(session_id: String) -> Self {
        Self {
            session_id,
            functions: udf::functions()
                .into_iter()
                .map(|inner| NativeScalarUdf { inner })
                .collect(),
        }
    }
    fn scalar_udfs(&self) -> Vec<NativeScalarUdf> {
        self.functions.clone()
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeScalarUdf>()?;
    module.add_class::<BoundSecondString>()?;
    Ok(())
}

#[cfg(test)]
mod tests;
