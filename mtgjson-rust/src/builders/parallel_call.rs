// MTGJSON parallel call - Ultra-high performance parallel processing using Rust async/tokio
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::sync::Semaphore;
use smallvec::SmallVec;
use rustc_hash::FxHashMap;
use std::collections::VecDeque;

#[pyfunction]
#[pyo3(signature = (function, args, repeatable_args=None, fold_list=false, fold_dict=false, force_starmap=false, pool_size=32))]
pub fn parallel_call(
    py: Python,
    function: PyObject,
    args: &Bound<'_, PyList>,
    repeatable_args: Option<&Bound<'_, PyList>>,
    fold_list: bool,
    fold_dict: bool,
    force_starmap: bool,
    pool_size: usize,
) -> PyResult<PyObject> {
    // Validate pool size for optimal performance
    let effective_pool_size = pool_size.clamp(1, 256);
    
    // Pre-allocate result storage based on input size
    let args_len = args.len();
    if args_len == 0 {
        return Ok(PyList::empty_bound(py).to_object(py));
    }
    
    // Create optimized Tokio runtime with custom configuration
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(effective_pool_size.min(num_cpus::get()))
        .max_blocking_threads(effective_pool_size)
        .enable_all()
        .build()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Failed to create optimized runtime: {}", e)
        ))?;
    
    rt.block_on(async {
        // Use Arc for efficient sharing across tasks
        let semaphore = Arc::new(Semaphore::new(effective_pool_size));
        let mut join_set = JoinSet::new();
        
        // Convert repeatable arguments to PyObjects if provided
        let repeat_objects: Option<Vec<PyObject>> = repeatable_args.map(|args| {
            args.iter().map(|arg| arg.to_object(py)).collect()
        });
        
        // Process tasks based on configuration for optimal performance
        match (&repeat_objects, force_starmap) {
            (Some(repeat_args), false) => {
                // With repeatable arguments: function(arg, *repeatable_args)
                
                for arg in args.iter() {
                    let func_ref = function.clone_ref(py);
                    let arg_obj = arg.to_object(py);
                    let repeat_clone: Vec<PyObject> = repeat_args.iter().map(|obj| obj.clone_ref(py)).collect();
                    let permit = Arc::clone(&semaphore);
                    
                    join_set.spawn(async move {
                        let _permit = permit.acquire().await.map_err(|e| 
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                format!("Semaphore error: {}", e)
                            )
                        )?;
                        
                        Python::with_gil(|py| -> PyResult<PyObject> {
                            // Build argument tuple efficiently
                            let mut call_args = Vec::<PyObject>::with_capacity(1 + repeat_clone.len());
                            call_args.push(arg_obj);
                            call_args.extend(repeat_clone);
                            
                            // Convert to PyTuple for function call
                            let args_tuple = PyTuple::new_bound(py, &call_args);
                            
                            // Call function(*g_args) matching Python behavior
                            func_ref.call1(py, args_tuple)
                        })
                    });
                }
            },
            (Some(repeat_args), true) => {
                // With repeatable arguments and starmap: function(*arg, *repeatable_args)
                
                for arg in args.iter() {
                    let func_ref = function.clone_ref(py);
                    let arg_obj = arg.to_object(py);
                    let repeat_clone: Vec<PyObject> = repeat_args.iter().map(|obj| obj.clone_ref(py)).collect();
                    let permit = Arc::clone(&semaphore);
                    
                    join_set.spawn(async move {
                        let _permit = permit.acquire().await.map_err(|e| 
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                format!("Semaphore error: {}", e)
                            )
                        )?;
                        
                        Python::with_gil(|py| -> PyResult<PyObject> {
                            // Handle starmap with repeatable args
                            if let Ok(tuple_arg) = arg_obj.downcast_bound::<PyTuple>(py) {
                                let mut all_args = Vec::new();
                                all_args.extend(tuple_arg.iter().map(|item| item.to_object(py)));
                                all_args.extend(repeat_clone);
                                let args_tuple = PyTuple::new_bound(py, &all_args);
                                func_ref.call1(py, args_tuple)
                            } else if let Ok(list_arg) = arg_obj.downcast_bound::<PyList>(py) {
                                let mut all_args = Vec::new();
                                all_args.extend(list_arg.iter().map(|item| item.to_object(py)));
                                all_args.extend(repeat_clone);
                                let args_tuple = PyTuple::new_bound(py, &all_args);
                                func_ref.call1(py, args_tuple)
                            } else {
                                // Single argument case with repeatable args
                                let mut all_args = vec![arg_obj];
                                all_args.extend(repeat_clone);
                                let args_tuple = PyTuple::new_bound(py, &all_args);
                                func_ref.call1(py, args_tuple)
                            }
                        })
                    });
                }
            },
            (None, true) => {
                // Optimized starmap case
                for arg in args.iter() {
                    let func_ref = function.clone_ref(py);
                    let arg_obj = arg.to_object(py);
                    let permit = Arc::clone(&semaphore);
                    
                    join_set.spawn(async move {
                        let _permit = permit.acquire().await.map_err(|e| 
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                format!("Semaphore error: {}", e)
                            )
                        )?;
                        
                        Python::with_gil(|py| -> PyResult<PyObject> {
                            // Handle starmap: function(*arg)
                            if let Ok(tuple_arg) = arg_obj.downcast_bound::<PyTuple>(py) {
                                func_ref.call1(py, tuple_arg)
                            } else if let Ok(list_arg) = arg_obj.downcast_bound::<PyList>(py) {
                                let tuple_arg = PyTuple::new_bound(py, list_arg.iter());
                                func_ref.call1(py, tuple_arg)
                            } else {
                                // Single argument case
                                func_ref.call1(py, (arg_obj,))
                            }
                        })
                    });
                }
            },
            (None, false) => {
                // Optimized normal case: function(arg)
                for arg in args.iter() {
                    let func_ref = function.clone_ref(py);
                    let arg_obj = arg.to_object(py);
                    let permit = Arc::clone(&semaphore);
                    
                    join_set.spawn(async move {
                        let _permit = permit.acquire().await.map_err(|e| 
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                format!("Semaphore error: {}", e)
                            )
                        )?;
                        
                        Python::with_gil(|py| -> PyResult<PyObject> {
                            func_ref.call1(py, (arg_obj,))
                        })
                    });
                }
            }
        }
        
        // Collect results with pre-allocated storage
        let mut results = Vec::with_capacity(args_len);
        let mut error_count = 0u32;
        
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(task_result) => {
                    match task_result {
                        Ok(value) => results.push(value),
                        Err(e) => {
                            error_count += 1;
                            if error_count == 1 {
                                // Return first error immediately for fail-fast behavior
                                return Err(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Task execution failed: {}", e)
                    ));
                }
            }
        }
        
        // Process results based on fold options with optimized implementations
        Python::with_gil(|py| -> PyResult<PyObject> {
            if fold_list {
                // Ultra-fast list flattening: list(itertools.chain.from_iterable(results))
                optimize_fold_list(py, results)
            } else if fold_dict {
                // Ultra-fast dict merging: dict(collections.ChainMap(*results))
                optimize_fold_dict(py, results)
            } else {
                // Return results as optimized list
                Ok(PyList::new_bound(py, results).to_object(py))
            }
        })
    })
}

/// Optimized list flattening with memory pre-allocation and fast iteration
#[inline]
fn optimize_fold_list(py: Python, results: Vec<PyObject>) -> PyResult<PyObject> {
    // Estimate total capacity to minimize reallocations
    let estimated_capacity = results.len() * 4; // Conservative estimate
    let mut flattened = Vec::with_capacity(estimated_capacity);
    
    for result in results {
        // Fast path for different Python types
        if let Ok(py_list) = result.downcast_bound::<PyList>(py) {
            // Direct list iteration - fastest path
            flattened.extend(py_list.iter().map(|item| item.to_object(py)));
        } else if let Ok(py_tuple) = result.downcast_bound::<PyTuple>(py) {
            // Tuple iteration
            flattened.extend(py_tuple.iter().map(|item| item.to_object(py)));
        } else {
            // Try generic iteration
            match result.bind(py).iter() {
                Ok(iter) => {
                    for item in iter {
                        flattened.push(item?.to_object(py));
                    }
                }
                Err(_) => {
                    // Not iterable, add as single item
                    flattened.push(result);
                }
            }
        }
    }
    
    Ok(PyList::new_bound(py, flattened).to_object(py))
}

/// Optimized dictionary merging with efficient key-value handling
#[inline]
fn optimize_fold_dict(py: Python, results: Vec<PyObject>) -> PyResult<PyObject> {
    // Use PyDict directly for maximum performance
    let result_dict = PyDict::new_bound(py);
    
    for result in results {
        if let Ok(dict) = result.downcast_bound::<PyDict>(py) {
            // Fast dictionary merging using PyDict's optimized iteration
            for (key, value) in dict.iter() {
                result_dict.set_item(key, value)?;
            }
        } else {
            // Handle other mapping types if needed
            if let Ok(items) = result.call_method0(py, "items") {
                if let Ok(items_iter) = items.bind(py).iter() {
                    for item in items_iter {
                        if let Ok(pair) = item?.downcast::<PyTuple>() {
                            if pair.len() == 2 {
                                result_dict.set_item(&pair.get_item(0)?, &pair.get_item(1)?)?;
                            }
                        }
                    }
                }
            }
        }
    }
    
    Ok(result_dict.to_object(py))
}

/// High-performance batch processor for large datasets
#[pyclass(name = "BatchProcessor")]
pub struct BatchProcessor {
    #[pyo3(get, set)]
    pub pool_size: usize,
    #[pyo3(get, set)]
    pub chunk_size: usize,
}

#[pymethods]
impl BatchProcessor {
    #[new]
    #[pyo3(signature = (pool_size=None, chunk_size=None))]
    pub fn new(pool_size: Option<usize>, chunk_size: Option<usize>) -> Self {
        Self {
            pool_size: pool_size.unwrap_or_else(|| num_cpus::get().max(4)),
            chunk_size: chunk_size.unwrap_or(1000),
        }
    }
    
    /// Process large datasets in optimized chunks
    #[pyo3(signature = (function, data, **kwargs))]
    pub fn process_batch(
        &self,
        py: Python,
        function: PyObject,
        data: &Bound<'_, PyList>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        let data_len = data.len();
        if data_len == 0 {
            return Ok(PyList::empty_bound(py).to_object(py));
        }
        
        // Calculate optimal chunk size based on data size and pool size
        let optimal_chunk_size = if data_len > self.chunk_size * self.pool_size {
            self.chunk_size
        } else {
            (data_len / self.pool_size).max(1)
        };
        
        // Create chunks efficiently
        let mut chunks = Vec::with_capacity((data_len + optimal_chunk_size - 1) / optimal_chunk_size);
        let mut start = 0;
        
        while start < data_len {
            let end = (start + optimal_chunk_size).min(data_len);
            let chunk: Vec<PyObject> = data.iter()
                .skip(start)
                .take(end - start)
                .map(|item| item.to_object(py))
                .collect();
            chunks.push(PyList::new_bound(py, chunk).to_object(py));
            start = end;
        }
        
        // Process chunks in parallel using optimized parallel_call
        let chunks_list = PyList::new_bound(py, chunks);
        parallel_call(
            py,
            function,
            &chunks_list,
            None,
            true, // fold_list to flatten results
            false,
            false,
            self.pool_size,
        )
    }
}

/// Ultra-fast async task queue for high-throughput processing
#[pyclass(name = "AsyncTaskQueue")]
pub struct AsyncTaskQueue {
    #[pyo3(get, set)]
    pub max_concurrent: usize,
    #[pyo3(get, set)]
    pub queue_size: usize,
}

#[pymethods]
impl AsyncTaskQueue {
    #[new]
    #[pyo3(signature = (max_concurrent=None, queue_size=None))]
    pub fn new(max_concurrent: Option<usize>, queue_size: Option<usize>) -> Self {
        Self {
            max_concurrent: max_concurrent.unwrap_or(64),
            queue_size: queue_size.unwrap_or(10000),
        }
    }
    
    /// Process tasks with advanced queue management
    pub fn process_queue(
        &self,
        py: Python,
        tasks: &Bound<'_, PyList>,
        processor: PyObject,
    ) -> PyResult<PyObject> {
        let task_count = tasks.len();
        if task_count == 0 {
            return Ok(PyList::empty_bound(py).to_object(py));
        }
        
        // Use parallel_call with optimized parameters
        parallel_call(
            py,
            processor,
            tasks,
            None,
            false,
            false,
            false,
            self.max_concurrent,
        )
    }
    
    /// Process with backpressure control
    pub fn process_with_backpressure(
        &self,
        py: Python,
        tasks: &Bound<'_, PyList>,
        processor: PyObject,
    ) -> PyResult<PyObject> {
        let task_count = tasks.len();
        
        // If task count exceeds queue size, process in batches
        if task_count > self.queue_size {
            let batch_processor = BatchProcessor::new(
                Some(self.max_concurrent),
                Some(self.queue_size),
            );
            batch_processor.process_batch(py, processor, tasks, None)
        } else {
            self.process_queue(py, tasks, processor)
        }
    }
}

/// Specialized processor for MTGJSON card building operations
#[pyclass(name = "CardBuildProcessor")]
pub struct CardBuildProcessor {
    #[pyo3(get, set)]
    pub pool_size: usize,
}

#[pymethods]
impl CardBuildProcessor {
    #[new]
    #[pyo3(signature = (pool_size=None))]
    pub fn new(pool_size: Option<usize>) -> Self {
        Self {
            pool_size: pool_size.unwrap_or(32),
        }
    }
    
    /// Optimized parallel card building
    pub fn build_cards_parallel(
        &self,
        py: Python,
        card_builder: PyObject,
        scryfall_objects: &Bound<'_, PyList>,
        repeatable_args: Option<&Bound<'_, PyList>>,
    ) -> PyResult<PyObject> {
        // Use optimized parallel_call with card-specific optimizations
        parallel_call(
            py,
            card_builder,
            scryfall_objects,
            repeatable_args,
            true, // fold_list - cards are returned as lists to be flattened
            false,
            false,
            self.pool_size,
        )
    }
    
    /// Process foreign data in parallel
    pub fn process_foreign_data_parallel(
        &self,
        py: Python,
        foreign_processor: PyObject,
        foreign_urls: &Bound<'_, PyList>,
        card_info: &Bound<'_, PyList>,
    ) -> PyResult<PyObject> {
        // Zip foreign URLs with card info for parallel processing
        let combined_args: Vec<PyObject> = foreign_urls.iter()
            .zip(card_info.iter())
            .map(|(url, info)| PyTuple::new_bound(py, &[url.to_object(py), info.to_object(py)]).to_object(py))
            .collect();
        
        let combined_list = PyList::new_bound(py, combined_args);
        
        parallel_call(
            py,
            foreign_processor,
            &combined_list,
            None,
            false,
            false,
            true, // force_starmap to unpack (url, info) tuples
            self.pool_size,
        )
    }
}

/// Missing alias types for backwards compatibility
pub type ParallelProcessor = BatchProcessor;
pub type ParallelIterator = AsyncTaskQueue;

/// Optimized parallel map function for simple transformations
#[pyfunction]
#[pyo3(signature = (function, iterable, pool_size=32))]
pub fn parallel_map(
    py: Python,
    function: PyObject,
    iterable: &Bound<'_, PyList>,
    pool_size: usize,
) -> PyResult<PyObject> {
    // Simple wrapper around parallel_call for map-like operations
    parallel_call(
        py,
        function,
        iterable,
        None,
        false,
        false,
        false,
        pool_size,
    )
}

/// Optimized parallel starmap function
#[pyfunction]
#[pyo3(signature = (function, iterable, pool_size=32))]
pub fn parallel_starmap(
    py: Python,
    function: PyObject,
    iterable: &Bound<'_, PyList>,
    pool_size: usize,
) -> PyResult<PyObject> {
    // Starmap version using parallel_call
    parallel_call(
        py,
        function,
        iterable,
        None,
        false,
        false,
        true, // force_starmap
        pool_size,
    )
}

/// Python module definition with optimized exports
#[pymodule]
pub fn parallel_call_module(m: &Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    // Main functions
    m.add_function(wrap_pyfunction!(parallel_call, m)?)?;
    m.add_function(wrap_pyfunction!(parallel_map, m)?)?;
    m.add_function(wrap_pyfunction!(parallel_starmap, m)?)?;
    
    // Specialized processors
    m.add_class::<BatchProcessor>()?;
    m.add_class::<AsyncTaskQueue>()?;
    m.add_class::<CardBuildProcessor>()?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyFunction;
    
    #[test]
    fn test_parallel_call_basic() {
        Python::with_gil(|py| {
            // Create a simple Python function for testing
            let code = "def test_func(x): return x * 2";
            let module = pyo3::types::PyModule::from_code_bound(py, code, "test.py", "test").unwrap();
            let func = module.getattr("test_func").unwrap().to_object(py);
            
            // Create test data
            let data = PyList::new_bound(py, &[1, 2, 3, 4, 5]);
            
            // Test parallel execution
            let result = parallel_call(
                py,
                func,
                &data,
                None,
                false,
                false,
                false,
                4,
            ).unwrap();
            
            // Verify results
            let result_list = result.downcast_bound::<PyList>(py).unwrap();
            assert_eq!(result_list.len(), 5);
        });
    }
    
    #[test]
    fn test_parallel_call_fold_list() {
        Python::with_gil(|py| {
            // Function that returns a list
            let code = "def list_func(x): return [x, x*2]";
            let module = pyo3::types::PyModule::from_code_bound(py, code, "test.py", "test").unwrap();
            let func = module.getattr("list_func").unwrap().to_object(py);
            
            let data = PyList::new_bound(py, &[1, 2, 3]);
            
            let result = parallel_call(
                py,
                func,
                &data,
                None,
                true, // fold_list
                false,
                false,
                4,
            ).unwrap();
            
            let result_list = result.downcast_bound::<PyList>(py).unwrap();
            assert_eq!(result_list.len(), 6); // 3 inputs * 2 outputs each
        });
    }
    
    #[test]
    fn test_batch_processor() {
        Python::with_gil(|py| {
            let processor = BatchProcessor::new(Some(4), Some(10));
            
            let code = "def batch_func(items): return [item * 2 for item in items]";
            let module = pyo3::types::PyModule::from_code_bound(py, code, "test.py", "test").unwrap();
            let func = module.getattr("batch_func").unwrap().to_object(py);
            
            let data = PyList::new_bound(py, &(1..=25).collect::<Vec<_>>());
            
            let result = processor.process_batch(py, func, &data, None).unwrap();
            let result_list = result.downcast_bound::<PyList>(py).unwrap();
            assert_eq!(result_list.len(), 25);
        });
    }
}