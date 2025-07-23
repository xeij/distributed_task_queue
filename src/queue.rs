//! Task queue implementation with Redis backend

use redis::aio::Connection;
use redis::{Client, RedisError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::error::{TaskError, TaskResult};
use crate::task::{TaskDefinition, TaskId, TaskPriority, TaskStatus};

/// Redis keys for different queue operations
const QUEUE_KEY: &str = "dtq:queue";
const SCHEDULED_KEY: &str = "dtq:scheduled";
const PROCESSING_KEY: &str = "dtq:processing";
const RESULTS_KEY: &str = "dtq:results";
const FAILED_KEY: &str = "dtq:failed";
const STATS_KEY: &str = "dtq:stats";

/// Configuration for the task queue
#[derive(Debug, Clone)]
pub struct TaskQueueConfig {
    /// Redis connection URL
    pub redis_url: String,
    /// Default queue name
    pub default_queue: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Task result TTL in seconds
    pub result_ttl: u64,
    /// Failed task TTL in seconds
    pub failed_ttl: u64,
    /// Cleanup interval in seconds
    pub cleanup_interval: u64,
}

impl Default for TaskQueueConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            default_queue: "default".to_string(),
            max_connections: 10,
            result_ttl: 86400, // 24 hours
            failed_ttl: 604800, // 7 days
            cleanup_interval: 3600, // 1 hour
        }
    }
}

/// Task queue statistics
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    pub pending_tasks: u64,
    pub processing_tasks: u64,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub scheduled_tasks: u64,
}

/// Distributed task queue with Redis backend
#[derive(Debug)]
pub struct TaskQueue {
    client: Client,
    config: TaskQueueConfig,
    connections: Arc<RwLock<HashMap<String, Connection>>>,
}

impl TaskQueue {
    /// Create a new task queue with the given configuration
    pub async fn new(config: TaskQueueConfig) -> TaskResult<Self> {
        let client = Client::open(config.redis_url.as_str())
            .map_err(|e| TaskError::queue_operation("connect", e.to_string()))?;

        // Test the connection
        let mut conn = client
            .get_async_connection()
            .await
            .map_err(|e| TaskError::queue_operation("connect", e.to_string()))?;

        // Test basic Redis operations
        redis::cmd("PING")
            .query_async::<_, String>(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("ping", e.to_string()))?;

        info!("Connected to Redis at {}", config.redis_url);

        Ok(Self {
            client,
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Create a new task queue with default configuration
    pub async fn new_default() -> TaskResult<Self> {
        Self::new(TaskQueueConfig::default()).await
    }

    /// Get a Redis connection
    async fn get_connection(&self) -> TaskResult<Connection> {
        self.client
            .get_async_connection()
            .await
            .map_err(|e| TaskError::queue_operation("get_connection", e.to_string()))
    }

    /// Submit a task to the queue
    pub async fn submit_task(&self, mut task_def: TaskDefinition) -> TaskResult<TaskId> {
        let mut conn = self.get_connection().await?;
        
        // Use default queue if not specified
        if task_def.queue.is_empty() {
            task_def.queue = self.config.default_queue.clone();
        }

        let task_json = serde_json::to_string(&task_def)?;
        let queue_key = format!("{}:{}", QUEUE_KEY, task_def.queue);
        let task_key = format!("{}:task:{}", QUEUE_KEY, task_def.id);
        
        // Add task to priority queue (using sorted set with priority as score)
        let priority_score = task_def.priority.clone() as i32;
        
        redis::pipe()
            .zadd(&queue_key, priority_score, &task_json)
            .ignore()
            .hset(
                &task_key,
                &[("data", &task_json)],
            )
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("submit", e.to_string()))?;

        debug!("Submitted task {} to queue {}", task_def.id, task_def.queue);
        Ok(task_def.id)
    }

    /// Submit a scheduled task
    pub async fn submit_scheduled_task(&self, mut task_def: TaskDefinition) -> TaskResult<TaskId> {
        let mut conn = self.get_connection().await?;
        
        if task_def.queue.is_empty() {
            task_def.queue = self.config.default_queue.clone();
        }

        let task_json = serde_json::to_string(&task_def)?;
        let task_key = format!("{}:task:{}", QUEUE_KEY, task_def.id);
        let scheduled_at_timestamp = task_def
            .scheduled_at
            .ok_or_else(|| TaskError::queue_operation("submit_scheduled", "missing scheduled_at"))?
            .timestamp();

        // Add to scheduled tasks sorted set
        redis::pipe()
            .zadd(SCHEDULED_KEY, scheduled_at_timestamp, &task_json)
            .ignore()
            .hset(
                &task_key,
                &[("data", &task_json)],
            )
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("submit_scheduled", e.to_string()))?;

        debug!("Submitted scheduled task {} for {:?}", task_def.id, task_def.scheduled_at);
        Ok(task_def.id)
    }

    /// Get the next task from a queue
    pub async fn get_next_task(&self, queue_name: &str) -> TaskResult<Option<TaskDefinition>> {
        let mut conn = self.get_connection().await?;
        let queue_key = format!("{}:{}", QUEUE_KEY, queue_name);

        // Get highest priority task (ZREVRANGE gets highest scores first)
        let tasks: Vec<String> = redis::cmd("ZREVRANGE")
            .arg(&queue_key)
            .arg(0)
            .arg(0)
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("get_next", e.to_string()))?;

        if let Some(task_json) = tasks.first() {
            let task_def: TaskDefinition = serde_json::from_str(task_json)?;
            
            // Move task to processing queue
            redis::pipe()
                .zrem(&queue_key, task_json)
                .ignore()
                .zadd(PROCESSING_KEY, chrono::Utc::now().timestamp(), task_json)
                .ignore()
                .query_async(&mut conn)
                .await
                .map_err(|e| TaskError::queue_operation("move_to_processing", e.to_string()))?;

            debug!("Retrieved task {} from queue {}", task_def.id, queue_name);
            Ok(Some(task_def))
        } else {
            Ok(None)
        }
    }

    /// Move scheduled tasks that are ready to the appropriate queues
    pub async fn process_scheduled_tasks(&self) -> TaskResult<u64> {
        let mut conn = self.get_connection().await?;
        let now = chrono::Utc::now().timestamp();

        // Get all tasks scheduled before now
        let scheduled_tasks: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(SCHEDULED_KEY)
            .arg("-inf")
            .arg(now)
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("get_scheduled", e.to_string()))?;

        let mut processed_count = 0;
        
        for task_json in scheduled_tasks {
            let mut task_def: TaskDefinition = serde_json::from_str(&task_json)?;
            task_def.status = TaskStatus::Pending;
            
            let updated_json = serde_json::to_string(&task_def)?;
            let queue_key = format!("{}:{}", QUEUE_KEY, task_def.queue);
            let priority_score = task_def.priority.clone() as i32;

            // Move from scheduled to queue
            redis::pipe()
                .zrem(SCHEDULED_KEY, &task_json)
                .ignore()
                .zadd(&queue_key, &updated_json, priority_score)
                .ignore()
                .hset(
                    format!("{}:task:{}", QUEUE_KEY, task_def.id),
                    &[("data", &updated_json)],
                )
                .ignore()
                .query_async(&mut conn)
                .await
                .map_err(|e| TaskError::queue_operation("move_scheduled", e.to_string()))?;

            processed_count += 1;
            debug!("Moved scheduled task {} to queue {}", task_def.id, task_def.queue);
        }

        if processed_count > 0 {
            info!("Processed {} scheduled tasks", processed_count);
        }

        Ok(processed_count)
    }

    /// Mark a task as completed
    pub async fn mark_task_completed(&self, task_def: &TaskDefinition) -> TaskResult<()> {
        let mut conn = self.get_connection().await?;
        let task_json = serde_json::to_string(task_def)?;

        redis::pipe()
            .zrem(PROCESSING_KEY, &task_json)
            .ignore()
            .hset(
                format!("{}:result:{}", RESULTS_KEY, task_def.id),
                &[("data", &task_json)],
            )
            .ignore()
            .expire(
                format!("{}:result:{}", RESULTS_KEY, task_def.id),
                self.config.result_ttl as usize,
            )
            .ignore()
            .hset(
                format!("{}:task:{}", QUEUE_KEY, task_def.id),
                &[("data", &task_json)],
            )
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("mark_completed", e.to_string()))?;

        debug!("Marked task {} as completed", task_def.id);
        Ok(())
    }

    /// Mark a task as failed
    pub async fn mark_task_failed(&self, task_def: &TaskDefinition) -> TaskResult<()> {
        let mut conn = self.get_connection().await?;
        let task_json = serde_json::to_string(task_def)?;

        redis::pipe()
            .zrem(PROCESSING_KEY, &task_json)
            .ignore()
            .hset(
                format!("{}:failed:{}", FAILED_KEY, task_def.id),
                &[("data", &task_json)],
            )
            .ignore()
            .expire(
                format!("{}:failed:{}", FAILED_KEY, task_def.id),
                self.config.failed_ttl as usize,
            )
            .ignore()
            .hset(
                format!("{}:task:{}", QUEUE_KEY, task_def.id),
                &[("data", &task_json)],
            )
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("mark_failed", e.to_string()))?;

        debug!("Marked task {} as failed", task_def.id);
        Ok(())
    }

    /// Requeue a task for retry
    pub async fn requeue_task(&self, task_def: &TaskDefinition) -> TaskResult<()> {
        if task_def.scheduled_at.is_some() {
            self.submit_scheduled_task(task_def.clone()).await?;
        } else {
            self.submit_task(task_def.clone()).await?;
        }
        
        debug!("Requeued task {} for retry", task_def.id);
        Ok(())
    }

    /// Get task by ID
    pub async fn get_task(&self, task_id: TaskId) -> TaskResult<Option<TaskDefinition>> {
        let mut conn = self.get_connection().await?;
        
        let task_data: Option<String> = redis::cmd("HGET")
            .arg(format!("{}:task:{}", QUEUE_KEY, task_id))
            .arg("data")
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("get_task", e.to_string()))?;

        match task_data {
            Some(json) => {
                let task_def: TaskDefinition = serde_json::from_str(&json)?;
                Ok(Some(task_def))
            }
            None => Ok(None),
        }
    }

    /// Get queue statistics
    pub async fn get_stats(&self, queue_name: &str) -> TaskResult<QueueStats> {
        let mut conn = self.get_connection().await?;
        let queue_key = format!("{}:{}", QUEUE_KEY, queue_name);

        let pending_tasks: u64 = redis::cmd("ZCARD")
            .arg(&queue_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("get_stats", e.to_string()))?;

        let processing_tasks: u64 = redis::cmd("ZCARD")
            .arg(PROCESSING_KEY)
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("get_stats", e.to_string()))?;

        let scheduled_tasks: u64 = redis::cmd("ZCARD")
            .arg(SCHEDULED_KEY)
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("get_stats", e.to_string()))?;

        Ok(QueueStats {
            pending_tasks,
            processing_tasks,
            scheduled_tasks,
            completed_tasks: 0, // Would need additional tracking
            failed_tasks: 0,    // Would need additional tracking
        })
    }

    /// List all available queues
    pub async fn list_queues(&self) -> TaskResult<Vec<String>> {
        let mut conn = self.get_connection().await?;
        
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{}:*", QUEUE_KEY))
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("list_queues", e.to_string()))?;

        let queues: Vec<String> = keys
            .into_iter()
            .filter_map(|key| {
                if let Some(queue_name) = key.strip_prefix(&format!("{}:", QUEUE_KEY)) {
                    if !queue_name.contains(':') {
                        Some(queue_name.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        Ok(queues)
    }

    /// Cleanup expired tasks and data
    pub async fn cleanup_expired_tasks(&self) -> TaskResult<u64> {
        let mut conn = self.get_connection().await?;
        let now = chrono::Utc::now().timestamp();
        let cutoff_time = now - (self.config.result_ttl as i64);

        // Remove old processing tasks (tasks stuck in processing state)
        let removed_count: u64 = redis::cmd("ZREMRANGEBYSCORE")
            .arg(PROCESSING_KEY)
            .arg("-inf")
            .arg(cutoff_time)
            .query_async(&mut conn)
            .await
            .map_err(|e| TaskError::queue_operation("cleanup", e.to_string()))?;

        if removed_count > 0 {
            warn!("Cleaned up {} stuck processing tasks", removed_count);
        }

        Ok(removed_count)
    }
} 