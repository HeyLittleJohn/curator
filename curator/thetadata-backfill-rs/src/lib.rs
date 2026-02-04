//! ThetaData Historical Backfill - Rust Implementation
//! 
//! High-performance data backfill system with Python bindings via PyO3.

pub mod config;
pub mod error;
pub mod models;
pub mod client;
pub mod db;
pub mod downloaders;
pub mod worker;

use pyo3::prelude::*;

/// Python module - provides backfill functionality to Python
#[pymodule]
fn thetadata_backfill(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyBackfillConfig>()?;
    m.add_class::<PyBackfillResult>()?;
    m.add_function(wrap_pyfunction!(run_backfill, m)?)?;
    m.add_function(wrap_pyfunction!(run_equity_backfill, m)?)?;
    m.add_function(wrap_pyfunction!(run_option_backfill, m)?)?;
    m.add_function(wrap_pyfunction!(run_index_backfill, m)?)?;
    Ok(())
}

/// Configuration for backfill operations (Python-accessible)
#[pyclass]
#[derive(Clone)]
pub struct PyBackfillConfig {
    #[pyo3(get, set)]
    pub database_url: String,
    #[pyo3(get, set)]
    pub api_base_url: String,
    #[pyo3(get, set)]
    pub worker_count: usize,
    #[pyo3(get, set)]
    pub batch_size: usize,
    #[pyo3(get, set)]
    pub mode: String, // "full" or "incremental"
}

#[pymethods]
impl PyBackfillConfig {
    #[new]
    #[pyo3(signature = (database_url, api_base_url="http://localhost:25503/v3".to_string(), worker_count=0, batch_size=1000, mode="full".to_string()))]
    fn new(
        database_url: String,
        api_base_url: String,
        worker_count: usize,
        batch_size: usize,
        mode: String,
    ) -> Self {
        Self {
            database_url,
            api_base_url,
            worker_count: if worker_count == 0 { num_cpus::get() } else { worker_count },
            batch_size,
            mode,
        }
    }
}

/// Result of a backfill operation (Python-accessible)
#[pyclass]
#[derive(Clone)]
pub struct PyBackfillResult {
    #[pyo3(get)]
    pub total_tasks: usize,
    #[pyo3(get)]
    pub completed_tasks: usize,
    #[pyo3(get)]
    pub failed_tasks: usize,
    #[pyo3(get)]
    pub total_records: usize,
    #[pyo3(get)]
    pub duration_seconds: f64,
    #[pyo3(get)]
    pub errors: Vec<String>,
}

#[pymethods]
impl PyBackfillResult {
    #[getter]
    fn success_rate(&self) -> f64 {
        if self.total_tasks == 0 {
            0.0
        } else {
            (self.total_tasks - self.failed_tasks) as f64 / self.total_tasks as f64 * 100.0
        }
    }
}

/// Run a complete backfill for all asset types
#[pyfunction]
#[pyo3(signature = (config, symbols=None, start_date=None, end_date=None))]
fn run_backfill(
    py: Python<'_>,
    config: PyBackfillConfig,
    symbols: Option<Vec<String>>,
    start_date: Option<String>,
    end_date: Option<String>,
) -> PyResult<PyBackfillResult> {
    py.allow_threads(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Implementation will call the orchestrator
            Ok(PyBackfillResult {
                total_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                total_records: 0,
                duration_seconds: 0.0,
                errors: vec![],
            })
        })
    })
}

/// Run equity backfill
#[pyfunction]
#[pyo3(signature = (config, symbols=None, start_date=None, end_date=None, data_types=None))]
fn run_equity_backfill(
    py: Python<'_>,
    config: PyBackfillConfig,
    symbols: Option<Vec<String>>,
    start_date: Option<String>,
    end_date: Option<String>,
    data_types: Option<Vec<String>>,
) -> PyResult<PyBackfillResult> {
    py.allow_threads(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            Ok(PyBackfillResult {
                total_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                total_records: 0,
                duration_seconds: 0.0,
                errors: vec![],
            })
        })
    })
}

/// Run options backfill with Greeks
#[pyfunction]
#[pyo3(signature = (config, symbols=None, start_date=None, end_date=None, data_types=None))]
fn run_option_backfill(
    py: Python<'_>,
    config: PyBackfillConfig,
    symbols: Option<Vec<String>>,
    start_date: Option<String>,
    end_date: Option<String>,
    data_types: Option<Vec<String>>,
) -> PyResult<PyBackfillResult> {
    py.allow_threads(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            Ok(PyBackfillResult {
                total_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                total_records: 0,
                duration_seconds: 0.0,
                errors: vec![],
            })
        })
    })
}

/// Run index backfill
#[pyfunction]
#[pyo3(signature = (config, symbols=None, start_date=None, end_date=None, data_types=None))]
fn run_index_backfill(
    py: Python<'_>,
    config: PyBackfillConfig,
    symbols: Option<Vec<String>>,
    start_date: Option<String>,
    end_date: Option<String>,
    data_types: Option<Vec<String>>,
) -> PyResult<PyBackfillResult> {
    py.allow_threads(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            Ok(PyBackfillResult {
                total_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                total_records: 0,
                duration_seconds: 0.0,
                errors: vec![],
            })
        })
    })
}
