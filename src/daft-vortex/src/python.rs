use daft_core::python::PySchema;
use pyo3::{prelude::*, wrap_pyfunction};

#[pyfunction]
fn read_vortex_schema(py: Python<'_>, uri: &str) -> PyResult<PySchema> {
    let uri = uri.to_string();
    py.detach(move || {
        let runtime = common_runtime::get_io_runtime(true);
        let schema = runtime
            .block_within_async_context(async move { crate::read_vortex_schema(&uri).await })??;
        Ok(schema.into())
    })
}

pub fn register_python(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(read_vortex_schema, parent)?)?;
    Ok(())
}
