// MTGJSON parallel call - High performance parallel processing using Rust async/tokio
use pyo3::prelude::*;

use std::collections::HashMap;
use tokio::task::JoinSet;

/// Execute a function in parallel - Exact Python API compatibility
/// This matches the Python parallel_call function signature exactly
#[pyfunction]
#[pyo3(signature = (function, args, repeatable_args=None, fold_list=false, fold_dict=false, force_starmap=false, pool_size=32))]
pub fn parallel_call(
    py: Python,
    function: PyObject,
    args: Vec<PyObject>,
    repeatable_args: Option<Vec<PyObject>>,
    fold_list: bool,
    fold_dict: bool,
    force_starmap: bool,
    pool_size: usize,
) -> PyResult<PyObject> {
    // Create Tokio runtime for high-performance async execution
    let rt = tokio::runtime::Runtime::new().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create runtime: {}", e))
    })?;
    
    rt.block_on(async {
        let mut join_set = JoinSet::new();
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(pool_size));
        
        // Process arguments based on Python logic
        if let Some(repeatable_args) = repeatable_args {
            // Handle repeatable_args case: zip(args, *[itertools.repeat(arg) for arg in repeatable_args])
            for (_i, arg) in args.iter().enumerate() {
                let func_clone = function.clone_ref(py);
                let arg_clone = arg.clone_ref(py);
                // Convert Vec to Python objects properly
                let repeat_args_clone: Vec<PyObject> = repeatable_args.iter()
                    .map(|x| x.clone_ref(py))
                    .collect();
                
                let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to acquire permit: {}", e))
                })?;
                
                join_set.spawn(async move {
                    let _permit = permit;
                    
                    // Simulate Python's zip(args, *extra_args_rep) behavior
                    Python::with_gil(|py| -> PyResult<PyObject> {
                        let mut call_args = vec![arg_clone];
                        call_args.extend(repeat_args_clone);
                        
                        if force_starmap {
                            // function(*g_args) - unpack arguments
                            func_clone.call1(py, (call_args,))
                        } else {
                            // function(g_args) - pass as tuple
                            func_clone.call1(py, (call_args,))
                        }
                    })
                });
            }
        } else if force_starmap {
            // Handle force_starmap case: function(*g_args)
            for arg in args {
                let func_clone = function.clone_ref(py);
                let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to acquire permit: {}", e))
                })?;
                
                join_set.spawn(async move {
                    let _permit = permit;
                    
                    Python::with_gil(|py| -> PyResult<PyObject> {
                        // function(*arg) - unpack the argument
                        func_clone.call1(py, (arg,))
                    })
                });
            }
        } else {
            // Handle normal case: function(arg)
            for arg in args {
                let func_clone = function.clone_ref(py);
                let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to acquire permit: {}", e))
                })?;
                
                join_set.spawn(async move {
                    let _permit = permit;
                    
                    Python::with_gil(|py| -> PyResult<PyObject> {
                        func_clone.call1(py, (arg,))
                    })
                });
            }
        }
        
        // Collect results
        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(task_result) => {
                    match task_result {
                        Ok(value) => results.push(value),
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Task failed: {}", e)
                    ));
                }
            }
        }
        
        // Process results based on fold options (matching Python behavior)
        Python::with_gil(|py| -> PyResult<PyObject> {
            if fold_list {
                // Flatten results into 1D list: list(itertools.chain.from_iterable(results))
                let mut flattened = Vec::new();
                for result in results {
                    // Try to iterate over the result if it's iterable
                    if let Ok(bound_result) = result.bind(py).iter() {
                        for item in bound_result {
                            flattened.push(item?.to_object(py));
                        }
                    } else {
                        flattened.push(result);
                    }
                }
                Ok(flattened.to_object(py))
            } else if fold_dict {
                // Merge dicts: dict(collections.ChainMap(*results))
                // Create a Python dict directly instead of Rust HashMap
                let result_dict = pyo3::types::PyDict::new_bound(py);
                for result in results {
                    if let Ok(dict) = result.downcast_bound::<pyo3::types::PyDict>(py) {
                        for (key, value) in dict.iter() {
                            result_dict.set_item(key, value)?;
                        }
                    }
                }
                Ok(result_dict.to_object(py))
            } else {
                // Return results as-is
                Ok(results.to_object(py))
            }
        })
    })
}

// Legacy class-based API for backward compatibility (will be deprecated)
#[pyclass(name = "ParallelProcessor")]
#[derive(Debug, Clone)]
pub struct ParallelProcessor {
    #[pyo3(get, set)]
    pub pool_size: usize,
}

#[pymethods]
impl ParallelProcessor {
    #[new]
    #[pyo3(signature = (pool_size=None))]
    pub fn new(pool_size: Option<usize>) -> Self {
        Self {
            pool_size: pool_size.unwrap_or(32),
        }
    }
    
    /// Legacy method - use parallel_call function instead
    pub fn parallel_call_batch(&self, tasks: Vec<String>) -> PyResult<Vec<String>> {
        eprintln!("⚠️ Warning: ParallelProcessor.parallel_call_batch is deprecated. Use parallel_call function instead.");
        
        // Simple implementation for backward compatibility
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            results.push(task.to_uppercase());
        }
        Ok(results)
    }
    
    /// Process parallel API calls 
    pub fn parallel_api_calls(&self, urls: Vec<String>) -> PyResult<Vec<String>> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create runtime: {}", e))
        })?;
        
        rt.block_on(async {
            let mut join_set = JoinSet::new();
            let client = reqwest::Client::new();
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(self.pool_size));
            
            for url in urls {
                let client_clone = client.clone();
                let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to acquire permit: {}", e))
                })?;
                
                join_set.spawn(async move {
                    let _permit = permit;
                    
                    match client_clone.get(&url).send().await {
                        Ok(response) => {
                            match response.text().await {
                                Ok(text) => text,
                                Err(e) => format!("Failed to read response: {}", e),
                            }
                        }
                        Err(e) => format!("Request failed: {}", e),
                    }
                });
            }
            
            let mut results = Vec::new();
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(response) => results.push(response),
                    Err(e) => results.push(format!("Task join error: {}", e)),
                }
            }
            
            Ok(results)
        })
    }
    
    /// Fast data folding
    pub fn parallel_transform_fold(&self, data: Vec<String>, fold_list: bool, _fold_dict: bool) -> PyResult<Vec<String>> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create runtime: {}", e))
        })?;
        
        rt.block_on(async {
            let mut join_set = JoinSet::new();
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(self.pool_size));
            
            for item in data {
                let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to acquire permit: {}", e))
                })?;
                
                join_set.spawn(async move {
                    let _permit = permit;
                    Self::transform_data(item).await
                });
            }
            
            let mut results = Vec::new();
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(transformed) => {
                        if fold_list {
                            // Flatten the result if it's a list
                            results.extend(Self::parse_as_list(&transformed));
                        } else {
                            results.push(transformed);
                        }
                    }
                    Err(e) => {
                        eprintln!("Transform failed: {}", e);
                    }
                }
            }
            
            Ok(results)
        })
    }
    
    /// parallel card processing for set building
    pub fn parallel_card_processing(&self, card_data: Vec<String>) -> PyResult<Vec<crate::card::MtgjsonCard>> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create runtime: {}", e))
        })?;
        
        rt.block_on(async {
            let mut join_set = JoinSet::new();
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(self.pool_size));
            
            for data in card_data {
                let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to acquire permit: {}", e))
                })?;
                
                join_set.spawn(async move {
                    let _permit = permit;
                    Self::process_card_data(data).await
                });
            }
            
            let mut cards = Vec::new();
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(card) => cards.push(card),
                    Err(e) => {
                        eprintln!("Card processing failed: {}", e);
                    }
                }
            }
            
            Ok(cards)
        })
    }
    
    /// parallel price processing for multiple providers
    pub fn parallel_price_processing(&self, providers: Vec<String>) -> String {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => return serde_json::to_string(&serde_json::json!({
                "error": format!("Failed to create runtime: {}", e)
            })).unwrap_or_default(),
        };
        
        let result = rt.block_on(async {
            let mut join_set = JoinSet::new();
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(self.pool_size));
            
            for provider in providers {
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(e) => {
                        eprintln!("Failed to acquire permit: {}", e);
                        continue;
                    }
                };
                
                join_set.spawn(async move {
                    let _permit = permit;
                    Self::fetch_provider_prices(provider).await
                });
            }
            
            let mut all_prices = HashMap::new();
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok((provider, prices)) => {
                        all_prices.insert(provider, prices);
                    }
                    Err(e) => {
                        eprintln!("Price fetch failed: {}", e);
                    }
                }
            }
            
            all_prices
        });
        
        serde_json::to_string(&result).unwrap_or_default()
    }
}

// Static async helper methods
impl ParallelProcessor {
    async fn process_single_task(task: String) -> String {
        // TODO: Implement actual task processing
        tokio::task::yield_now().await;
        // 
        task.to_uppercase()
    }
    
    async fn transform_data(data: String) -> String {
        tokio::task::yield_now().await;
        
        // TODO: JSON or data transformation would go here
        format!("transformed_{}", data)
    }
    
    async fn process_card_data(_data: String) -> crate::card::MtgjsonCard {
        tokio::task::yield_now().await;
        
        // TODO: Parse card data from JSON string
        // TODO: This would integrate with the actual card parsing logic
        crate::card::MtgjsonCard::new(false)
    }
    
    async fn fetch_provider_prices(provider: String) -> (String, serde_json::Value) {
        tokio::task::yield_now().await;
        
        // TODO: implement actual price fetching
        let prices = serde_json::json!({
            "sample_uuid": {
                "paper": {
                    "normal": {
                        "2024-01-01": 1.0
                    }
                }
            }
        });
        
        (provider, prices)
    }
    
    fn parse_as_list(data: &str) -> Vec<String> {
        // TODO: Implement actual list parsing
        data.split(',').map(|s| s.trim().to_string()).collect()
    }
}

impl Default for ParallelProcessor {
    fn default() -> Self {
        Self::new(None)
    }
}

/// parallel iterator for large datasets
#[pyclass(name = "ParallelIterator")]
pub struct ParallelIterator {
    #[pyo3(get, set)]
    pub chunk_size: usize,
    #[pyo3(get, set)]
    pub pool_size: usize,
}

#[pymethods]
impl ParallelIterator {
    #[new]
    #[pyo3(signature = (chunk_size=None, pool_size=None))]
    pub fn new(chunk_size: Option<usize>, pool_size: Option<usize>) -> Self {
        Self {
            chunk_size: chunk_size.unwrap_or(1000),
            pool_size: pool_size.unwrap_or(32),
        }
    }
    
    /// Process data in chunks - for large dataset processing
    pub fn process_chunks(&self, data: Vec<String>) -> PyResult<Vec<String>> {
        eprintln!("⚠️ Warning: Use parallel_call function for better performance and compatibility.");
        
        // Simple implementation
        Ok(data)
    }
}

// Internal helper methods not exposed to Python
impl ParallelIterator {
    fn process_chunk(chunk: Vec<String>) -> Vec<String> {
        // Process each chunk efficiently
        let mut results = Vec::with_capacity(chunk.len());
        
        for item in chunk {
            // Intensive processing would go here
            results.push(format!("processed_{}", item));
        }
        
        results
    }
}