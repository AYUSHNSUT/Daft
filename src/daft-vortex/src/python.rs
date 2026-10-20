use common_io_config::python::IOConfig;
use daft_core::{prelude::Field, python::PySchema};
use daft_dsl::python::PyExpr;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyDict, PyInt},
    wrap_pyfunction,
};
use vortex::{VortexSessionDefault, arrow::ArrowSessionExt, session::VortexSession};

use crate::{
    VortexConvertOptions, VortexReadOptions, plan::plan_scan, read::NULLABILITY_METADATA_KEY,
};

fn strict_vortex_budget(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "Vortex request budgets must be positive integers, not bool",
        ));
    }
    let budget = value.cast::<PyInt>()?.extract::<usize>()?;
    if budget == 0 {
        return Err(PyValueError::new_err(
            "Vortex request budgets must be positive integers",
        ));
    }
    Ok(budget)
}

#[pyfunction(signature = (uri, io_config=None, *, max_concurrent_requests=8, max_in_flight_bytes=67108864))]
fn read_vortex_schema(
    py: Python<'_>,
    uri: &str,
    io_config: Option<IOConfig>,
    #[pyo3(from_py_with = strict_vortex_budget)] max_concurrent_requests: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] max_in_flight_bytes: usize,
) -> PyResult<PySchema> {
    read_vortex_metadata(
        py,
        uri,
        io_config,
        max_concurrent_requests,
        max_in_flight_bytes,
    )
    .map(|(schema, _)| schema)
}

#[pyfunction(signature = (uri, io_config=None, *, max_concurrent_requests=8, max_in_flight_bytes=67108864))]
fn read_vortex_metadata(
    py: Python<'_>,
    uri: &str,
    io_config: Option<IOConfig>,
    #[pyo3(from_py_with = strict_vortex_budget)] max_concurrent_requests: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] max_in_flight_bytes: usize,
) -> PyResult<(PySchema, u64)> {
    let uri = uri.to_string();
    py.detach(move || {
        let options = VortexReadOptions {
            max_concurrent_requests,
            max_in_flight_bytes,
            ..Default::default()
        };
        crate::io::validate_options(&options)?;
        let client = daft_io::get_io_client(true, io_config.unwrap_or_default().config.into())?;
        let stats = daft_io::IOStatsContext::new("Vortex metadata inference");
        let runtime = common_runtime::get_io_runtime(true);
        let (schema, num_rows) = runtime.block_within_async_context(async move {
            crate::read_vortex_metadata(&uri, client, stats, options).await
        })??;
        Ok((schema.into(), num_rows))
    })
}

/// Inspect the same scan plan used for execution, without opening a file.
/// The caller supplies the complete native schema, including source nullability.
#[pyfunction(signature = (
    schema,
    columns=None,
    predicate=None,
    *,
    enable_projection_pushdown=true,
    enable_filter_pushdown=true,
))]
fn _vortex_scan_diagnostics(
    py: Python<'_>,
    schema: PySchema,
    columns: Option<Vec<String>>,
    predicate: Option<PyExpr>,
    enable_projection_pushdown: bool,
    enable_filter_pushdown: bool,
) -> PyResult<Bound<'_, PyDict>> {
    let arrow_schema = schema.schema.to_arrow()?;
    let fields = arrow_schema
        .fields()
        .iter()
        .zip(schema.schema.fields())
        .map(|(arrow, source)| {
            Ok(arrow
                .as_ref()
                .clone()
                .with_nullable(source_nullability(source)?))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let session = VortexSession::default();
    let dtype = session
        .arrow()
        .from_arrow_schema(&arrow_schema::Schema::new(fields))
        .map_err(|error| PyValueError::new_err(format!("Vortex diagnostic schema: {error}")))?;
    let (_, actual_schema) = crate::read::schemas("diagnostics", &session, &dtype)?;
    crate::read::validate_schema("diagnostics", &schema.schema, &actual_schema)?;
    let read_options = VortexReadOptions {
        enable_projection_pushdown,
        enable_filter_pushdown,
        ..Default::default()
    };
    let convert_options = VortexConvertOptions {
        schema: schema.schema,
        predicate: predicate.map(|predicate| predicate.expr),
        include_columns: columns,
        limit: None,
    };
    let plan = plan_scan(&actual_schema, &dtype, &read_options, &convert_options)?;
    let result = PyDict::new(py);
    result.set_item("output_columns", plan.output_columns)?;
    result.set_item("physical_columns", plan.physical_columns)?;
    result.set_item(
        "pushed_filter",
        plan.pushed_filter.as_ref().map(ToString::to_string),
    )?;
    result.set_item(
        "residual_filter",
        plan.residual.as_ref().map(ToString::to_string),
    )?;
    result.set_item("filter_fallbacks", plan.filter_fallbacks)?;
    result.set_item("enable_projection_pushdown", enable_projection_pushdown)?;
    result.set_item("enable_filter_pushdown", enable_filter_pushdown)?;
    Ok(result)
}

fn source_nullability(field: &Field) -> PyResult<bool> {
    match field
        .metadata
        .get(NULLABILITY_METADATA_KEY)
        .map(String::as_str)
    {
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        _ => Err(PyValueError::new_err(format!(
            "Vortex diagnostics need source nullability for {:?}; use read_vortex_schema",
            field.name,
        ))),
    }
}

pub fn register_python(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(read_vortex_schema, parent)?)?;
    parent.add_function(wrap_pyfunction!(read_vortex_metadata, parent)?)?;
    parent.add_function(wrap_pyfunction!(_vortex_scan_diagnostics, parent)?)?;
    Ok(())
}
