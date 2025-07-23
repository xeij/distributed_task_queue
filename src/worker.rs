//! Worker implementation for processing tasks

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Mutex};
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

use crate::error::{TaskError, TaskResult};
use crate::queue::TaskQueue;
use crate::task::{Task, TaskDefinition, TaskStatus};

/// Unique identifier for workers
pub type WorkerId = Uuid;

/// Worker configuration
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Unique worker identifier
    pub worker_id: WorkerId,
    /// Queues this worker will process
    pub queues: Vec<String>,
    /// Maximum number of concurrent tasks
    pub max_concurrent_tasks: usize,
    /// Polling interval for new tasks in milliseconds
    pub polling_interval_ms: u64,
    /// Task execution timeout in seconds
    pub task_timeout: u64,
    /// Whether to auto-retry failed tasks
    pub auto_retry: bool,
    /// Heartbeat interval in seconds
    pub heartbeat_interval: u64,
    /// Worker shutdown grace period in seconds
    pub shutdown_grace_period: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: WorkerId::new_v4(),
            queues: vec!["default".to_string()],
            max_concurrent_tasks: 4,
            polling_interval_ms: 1000,
            task_timeout: 300, // 5 minutes
            auto_retry: true,
            heartbeat_interval: 30,
            shutdown_grace_period: 30,
        }
    }
}

/// Worker statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerStats {
    pub tasks_processed: u64,
    pub tasks_successful: u64,
    pub tasks_failed: u64,
    pub tasks_retried: u64,
    pub average_execution_time_ms: f64,
    pub last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,
    pub started_at: chrono::DateTime<chrono::Utc>,
}

/// Task handler trait for executing different types of tasks
#[async_trait::async_trait]
pub trait TaskHandler: Send + Sync {
    fn can_handle(&self, task_name: &str) -> bool;
    async fn handle(&self, task_data: &str) -> TaskResult<String>;
}

/// Registry for task handlers
#[derive(Default)]
pub struct TaskHandlerRegistry {
    handlers: RwLock<HashMap<String, Arc<dyn TaskHandler>>>,
}

impl TaskHandlerRegistry {
    /// Register a task handler for a specific task type
    pub async fn register<H>(&self, task_name: String, handler: H)
    where
        H: TaskHandler + 'static,
    {
        let mut handlers = self.handlers.write().await;
        handlers.insert(task_name, Arc::new(handler));
    }

    /// Find a handler for a task
    async fn find_handler(&self, task_name: &str) -> Option<Arc<dyn TaskHandler>> {
        let handlers = self.handlers.read().await;
        
        // First try exact match
        if let Some(handler) = handlers.get(task_name) {
            return Some(handler.clone());
        }

        // Then try handlers that can handle this task type
        for handler in handlers.values() {
            if handler.can_handle(task_name) {
                return Some(handler.clone());
            }
        }

        None
    }
}

/// Worker for processing tasks from the queue
pub struct Worker {
    config: WorkerConfig,
    queue: Arc<TaskQueue>,
    handlers: Arc<TaskHandlerRegistry>,
    stats: Arc<Mutex<WorkerStats>>,
    shutdown_signal: Arc<RwLock<bool>>,
    active_tasks: Arc<RwLock<HashMap<Uuid, tokio::task::JoinHandle<()>>>>,
}

impl Worker {
    /// Create a new worker with the given configuration
    pub fn new(config: WorkerConfig, queue: Arc<TaskQueue>) -> Self {
        let mut stats = WorkerStats::default();
        stats.started_at = chrono::Utc::now();

        Self {
            config,
            queue,
            handlers: Arc::new(TaskHandlerRegistry::default()),
            stats: Arc::new(Mutex::new(stats)),
            shutdown_signal: Arc::new(RwLock::new(false)),
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a task handler
    pub async fn register_handler<H>(&self, task_name: String, handler: H)
    where
        H: TaskHandler + 'static,
    {
        self.handlers.register(task_name, handler).await;
    }

    /// Start the worker
    pub async fn start(&self) -> TaskResult<()> {
        info!("Starting worker {} for queues: {:?}", self.config.worker_id, self.config.queues);

        // Start heartbeat task
        let heartbeat_task = self.start_heartbeat_task().await;

        // Start scheduled task processor
        let scheduler_task = self.start_scheduler_task().await;

        // Start cleanup task
        let cleanup_task = self.start_cleanup_task().await;

        // Main worker loop
        let worker_task = self.start_worker_loop().await;

        // Wait for shutdown signal or task completion
        tokio::select! {
            _ = heartbeat_task => {
                warn!("Heartbeat task completed unexpectedly");
            }
            _ = scheduler_task => {
                warn!("Scheduler task completed unexpectedly");
            }
            _ = cleanup_task => {
                warn!("Cleanup task completed unexpectedly");
            }
            _ = worker_task => {
                info!("Worker loop completed");
            }
        }

        // Graceful shutdown
        self.shutdown().await?;

        Ok(())
    }

    /// Start the main worker loop
    async fn start_worker_loop(&self) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let queue = self.queue.clone();
        let handlers = self.handlers.clone();
        let stats = self.stats.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        let active_tasks = self.active_tasks.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(config.polling_interval_ms));

            loop {
                interval.tick().await;

                // Check shutdown signal
                if *shutdown_signal.read().await {
                    break;
                }

                // Check if we can process more tasks
                let active_count = active_tasks.read().await.len();
                if active_count >= config.max_concurrent_tasks {
                    continue;
                }

                // Try to get a task from each queue
                for queue_name in &config.queues {
                    if let Ok(Some(mut task_def)) = queue.get_next_task(queue_name).await {
                        debug!("Got task {} from queue {}", task_def.id, queue_name);

                        // Mark task as started
                        task_def.mark_started(config.worker_id.to_string());

                                                 // Find handler for this task
                         if let Some(handler) = handlers.find_handler(&task_def.name).await {
                             let task_id = task_def.id;
                             
                             // Spawn task execution
                             let task_handle = Self::spawn_task_execution(
                                 task_def,
                                 handler,
                                 queue.clone(),
                                 stats.clone(),
                                 config.clone(),
                             ).await;

                             // Track active task
                             active_tasks.write().await.insert(task_id, task_handle);
                         } else {
                             error!("No handler found for task type: {}", task_def.name);
                             task_def.mark_failed(&format!("No handler found for task type: {}", task_def.name));
                             if let Err(e) = queue.mark_task_failed(&task_def).await {
                                 error!("Failed to mark task as failed: {}", e);
                             }
                         }
                    }
                }

                // Clean up completed tasks
                Self::cleanup_completed_tasks(&active_tasks).await;
            }

            info!("Worker loop shutting down");
        })
    }

    /// Spawn task execution in a separate task
    async fn spawn_task_execution(
        mut task_def: TaskDefinition,
        handler: Arc<dyn TaskHandler>,
        queue: Arc<TaskQueue>,
        stats: Arc<Mutex<WorkerStats>>,
        config: WorkerConfig,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            
            // Execute task with timeout
            let execution_result = tokio::time::timeout(
                Duration::from_secs(config.task_timeout),
                handler.handle(&task_def.data)
            ).await;

            let execution_duration = start_time.elapsed();

            // Update statistics
            {
                let mut stats = stats.lock().await;
                stats.tasks_processed += 1;
                
                // Update average execution time
                let new_avg = if stats.tasks_processed == 1 {
                    execution_duration.as_millis() as f64
                } else {
                    (stats.average_execution_time_ms * (stats.tasks_processed - 1) as f64 
                        + execution_duration.as_millis() as f64) / stats.tasks_processed as f64
                };
                stats.average_execution_time_ms = new_avg;
            }

            // Handle execution result
            match execution_result {
                Ok(Ok(result)) => {
                    // Task succeeded
                    if let Err(e) = task_def.mark_success(&result) {
                        error!("Failed to serialize task result: {}", e);
                        task_def.mark_failed(&format!("Failed to serialize result: {}", e));
                    }

                    let mut stats = stats.lock().await;
                    stats.tasks_successful += 1;

                    if let Err(e) = queue.mark_task_completed(&task_def).await {
                        error!("Failed to mark task as completed: {}", e);
                    }

                    info!("Task {} completed successfully in {:?}", task_def.id, execution_duration);
                }
                Ok(Err(e)) => {
                    // Task failed
                    let error_msg = e.to_string();
                    error!("Task {} failed: {}", task_def.id, error_msg);

                    // Try to retry if configured and possible
                    if config.auto_retry && task_def.can_retry() {
                        if let Ok(()) = task_def.mark_retry() {
                            if let Err(e) = queue.requeue_task(&task_def).await {
                                error!("Failed to requeue task for retry: {}", e);
                                task_def.mark_failed(&error_msg);
                                if let Err(e) = queue.mark_task_failed(&task_def).await {
                                    error!("Failed to mark task as failed: {}", e);
                                }
                            } else {
                                let mut stats = stats.lock().await;
                                stats.tasks_retried += 1;
                                info!("Task {} queued for retry (attempt {})", task_def.id, task_def.retry_count);
                                return;
                            }
                        }
                    }

                    task_def.mark_failed(&error_msg);
                    let mut stats = stats.lock().await;
                    stats.tasks_failed += 1;

                    if let Err(e) = queue.mark_task_failed(&task_def).await {
                        error!("Failed to mark task as failed: {}", e);
                    }
                }
                Err(_) => {
                    // Task timed out
                    let error_msg = format!("Task execution timed out after {} seconds", config.task_timeout);
                    error!("Task {} timed out", task_def.id);

                    task_def.mark_failed(&error_msg);
                    let mut stats = stats.lock().await;
                    stats.tasks_failed += 1;

                    if let Err(e) = queue.mark_task_failed(&task_def).await {
                        error!("Failed to mark task as failed: {}", e);
                    }
                }
            }
        })
    }

    /// Clean up completed task handles
    async fn cleanup_completed_tasks(active_tasks: &Arc<RwLock<HashMap<Uuid, tokio::task::JoinHandle<()>>>>) {
        let mut tasks = active_tasks.write().await;
        let mut completed_ids = Vec::new();

        for (task_id, handle) in tasks.iter() {
            if handle.is_finished() {
                completed_ids.push(*task_id);
            }
        }

        for task_id in completed_ids {
            tasks.remove(&task_id);
        }
    }

    /// Start heartbeat task
    async fn start_heartbeat_task(&self) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let stats = self.stats.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(config.heartbeat_interval));

            loop {
                interval.tick().await;

                if *shutdown_signal.read().await {
                    break;
                }

                // Update heartbeat timestamp
                {
                    let mut stats = stats.lock().await;
                    stats.last_heartbeat = Some(chrono::Utc::now());
                }

                debug!("Worker {} heartbeat", config.worker_id);
            }
        })
    }

    /// Start scheduler task (for processing scheduled tasks)
    async fn start_scheduler_task(&self) -> tokio::task::JoinHandle<()> {
        let queue = self.queue.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10)); // Check every 10 seconds

            loop {
                interval.tick().await;

                if *shutdown_signal.read().await {
                    break;
                }

                if let Err(e) = queue.process_scheduled_tasks().await {
                    error!("Failed to process scheduled tasks: {}", e);
                }
            }
        })
    }

    /// Start cleanup task
    async fn start_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let queue = self.queue.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(3600)); // Check every hour

            loop {
                interval.tick().await;

                if *shutdown_signal.read().await {
                    break;
                }

                if let Err(e) = queue.cleanup_expired_tasks().await {
                    error!("Failed to cleanup expired tasks: {}", e);
                }
            }
        })
    }

    /// Get worker statistics
    pub async fn get_stats(&self) -> WorkerStats {
        self.stats.lock().await.clone()
    }

    /// Signal worker to shutdown
    pub async fn signal_shutdown(&self) {
        let mut shutdown = self.shutdown_signal.write().await;
        *shutdown = true;
    }

    /// Graceful shutdown
    async fn shutdown(&self) -> TaskResult<()> {
        info!("Shutting down worker {}", self.config.worker_id);

        // Signal shutdown
        self.signal_shutdown().await;

        // Wait for active tasks to complete or timeout
        let start = std::time::Instant::now();
        let grace_period = Duration::from_secs(self.config.shutdown_grace_period);

        while start.elapsed() < grace_period {
            let active_count = self.active_tasks.read().await.len();
            if active_count == 0 {
                break;
            }

            debug!("Waiting for {} active tasks to complete", active_count);
            sleep(Duration::from_millis(500)).await;
        }

        // Force shutdown remaining tasks
        let active_tasks = self.active_tasks.read().await;
        for (task_id, handle) in active_tasks.iter() {
            warn!("Force stopping task {}", task_id);
            handle.abort();
        }

        info!("Worker {} shut down complete", self.config.worker_id);
        Ok(())
    }
} 