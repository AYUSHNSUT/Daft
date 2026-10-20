pub use common_io_config::python::{AzureConfig, GCSConfig, IOConfig};
pub use py::register_modules;

mod py {
    use common_error::DaftResult;
    use common_runtime::get_io_runtime;
    use futures::{StreamExt, TryStreamExt};
    use pyo3::{
        exceptions::{PyTypeError, PyValueError},
        prelude::*,
        types::{PyBool, PyDict, PyInt},
    };

    use crate::{get_io_client, parse_url, s3_like, stats::IOStatsContext};

    #[pyfunction(signature = (
        input,
        multithreaded_io=None,
        io_config=None,
        fanout_limit=None,
        page_size=None,
        limit=None
    ))]
    fn io_glob(
        py: Python,
        input: String,
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
    ) -> PyResult<Vec<Bound<PyDict>>> {
        let multithreaded_io = multithreaded_io.unwrap_or(true);
        let io_stats = IOStatsContext::new(format!("io_glob for {input}"));
        let io_stats_handle = io_stats;

        let lsr: DaftResult<Vec<_>> = py.detach(|| {
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let (_, path) = parse_url(&input)?;
            let runtime_handle = get_io_runtime(multithreaded_io);

            runtime_handle.block_on_current_thread(async {
                let source = io_client.get_source(&input).await?;
                let files = source
                    .glob(
                        path.as_ref(),
                        fanout_limit,
                        page_size,
                        limit,
                        Some(io_stats_handle),
                        None,
                    )
                    .await?
                    .try_collect()
                    .await?;
                Ok(files)
            })
        });
        let mut to_rtn = vec![];
        for file in lsr? {
            let dict = PyDict::new(py);
            dict.set_item("type", format!("{:?}", file.filetype))?;
            dict.set_item("path", file.filepath)?;
            dict.set_item("size", file.size)?;
            to_rtn.push(dict);
        }
        Ok(to_rtn)
    }

    type PyGlobBatch<'py> = (Vec<Bound<'py, PyDict>>, Option<Bound<'py, PyDict>>);

    fn strict_discovery_concurrency(value: &Bound<'_, PyAny>) -> PyResult<usize> {
        if value.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(
                "max_concurrent_inputs must be an integer, not bool",
            ));
        }
        let value = value.cast::<PyInt>()?.extract::<usize>()?;
        if !(1..=64).contains(&value) {
            return Err(PyValueError::new_err(
                "max_concurrent_inputs must be from 1 through 64",
            ));
        }
        Ok(value)
    }

    /// Ordered, bounded discovery with independent per-input failures.
    #[pyfunction(signature = (inputs, io_config=None, *, max_concurrent_inputs=8, collect_metrics=false))]
    fn _io_glob_bulk(
        py: Python<'_>,
        inputs: Vec<String>,
        io_config: Option<common_io_config::python::IOConfig>,
        #[pyo3(from_py_with = strict_discovery_concurrency)] max_concurrent_inputs: usize,
        collect_metrics: bool,
    ) -> PyResult<PyGlobBatch<'_>> {
        let num_inputs = inputs.len();
        let stats = collect_metrics.then(|| IOStatsContext::new("Vortex discovery"));
        let stats_handle = stats.clone();
        let started = collect_metrics.then(std::time::Instant::now);
        let outcomes: DaftResult<Vec<crate::Result<Vec<crate::object_io::FileMetadata>>>> = py
            .detach(|| {
                let client = get_io_client(true, io_config.unwrap_or_default().config.into())?;
                get_io_runtime(true).block_on_current_thread(async {
                    Ok(futures::stream::iter(inputs.into_iter().map(|input| {
                        let client = client.clone();
                        let stats = stats_handle.clone();
                        async move {
                            let (_, path) = parse_url(&input)?;
                            let source = client.get_source(&input).await?;
                            source
                                .glob(path.as_ref(), None, None, None, stats, None)
                                .await?
                                .try_collect()
                                .await
                        }
                    }))
                    .buffered(max_concurrent_inputs)
                    .collect()
                    .await)
                })
            });
        let mut results = Vec::with_capacity(num_inputs);
        for outcome in outcomes? {
            let result = PyDict::new(py);
            match outcome {
                Ok(files) => {
                    let mut entries = Vec::with_capacity(files.len());
                    for file in files {
                        let entry = PyDict::new(py);
                        entry.set_item("type", format!("{:?}", file.filetype))?;
                        entry.set_item("path", file.filepath)?;
                        entry.set_item("size", file.size)?;
                        entries.push(entry);
                    }
                    result.set_item("files", entries)?;
                }
                Err(error) => {
                    result.set_item(
                        "error",
                        PyErr::from(common_error::DaftError::from(error)).into_value(py),
                    )?;
                }
            }
            results.push(result);
        }
        let metrics = if let Some(stats) = stats {
            let metrics = PyDict::new(py);
            metrics.set_item("num_inputs", num_inputs)?;
            metrics.set_item("max_concurrent_inputs", max_concurrent_inputs)?;
            metrics.set_item("elapsed_seconds", started.unwrap().elapsed().as_secs_f64())?;
            metrics.set_item("head_requests", stats.load_head_requests())?;
            metrics.set_item("get_requests", stats.load_get_requests())?;
            metrics.set_item("list_requests", stats.load_list_requests())?;
            metrics.set_item("bytes_read", stats.load_bytes_read())?;
            Some(metrics)
        } else {
            None
        };
        Ok((results, metrics))
    }

    /// Creates an S3Config from the current environment, auto-discovering variables such as
    /// credentials, regions and more.
    #[pyfunction]
    fn s3_config_from_env(py: Python) -> PyResult<common_io_config::python::S3Config> {
        let s3_config: DaftResult<common_io_config::S3Config> = py.detach(|| {
            let runtime = get_io_runtime(false);
            runtime.block_on_current_thread(async { Ok(s3_like::s3_config_from_env().await?) })
        });
        Ok(common_io_config::python::S3Config { config: s3_config? })
    }

    #[pyfunction(signature = (
        path,
        data,
        multithreaded_io=None,
        io_config=None
    ))]
    fn io_put(
        py: Python,
        path: String,
        data: &[u8],
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<()> {
        let multithreaded_io = multithreaded_io.unwrap_or(true);
        let io_stats = IOStatsContext::new(format!("io_put for {path}"));
        let io_stats_handle = io_stats;

        let result: DaftResult<()> = py.detach(|| {
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )
            .map_err(|e| common_error::DaftError::External(e.into()))?;

            // Check if we're already in a runtime context
            let data_bytes = bytes::Bytes::copy_from_slice(data);
            match tokio::runtime::Handle::try_current() {
                Ok(_handle) => {
                    // We're in an async context, spawn a blocking task
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async {
                            io_client
                                .single_url_put(&path, data_bytes, Some(io_stats_handle))
                                .await
                                .map_err(|e| common_error::DaftError::External(e.into()))
                        })
                    })
                    .join()
                    .map_err(|_| common_error::DaftError::External("Thread join failed".into()))?
                }
                Err(_) => {
                    // No runtime, create one
                    let runtime_handle = get_io_runtime(multithreaded_io);
                    runtime_handle.block_on_current_thread(async {
                        io_client
                            .single_url_put(&path, data_bytes, Some(io_stats_handle))
                            .await
                            .map_err(|e| common_error::DaftError::External(e.into()))
                    })
                }
            }
        });
        result?;
        Ok(())
    }

    pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
        common_io_config::python::register_modules(parent)?;
        parent.add_function(wrap_pyfunction!(io_glob, parent)?)?;
        parent.add_function(wrap_pyfunction!(_io_glob_bulk, parent)?)?;
        parent.add_function(wrap_pyfunction!(s3_config_from_env, parent)?)?;
        parent.add_function(wrap_pyfunction!(io_put, parent)?)?;
        Ok(())
    }
}
