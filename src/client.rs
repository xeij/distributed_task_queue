//! Client interface for submitting tasks to the queue

use chrono::{DateTime, Utc};
use serde::Serialize;
use std::sync::Arc;

use crate::error::{TaskError, TaskResult};
use crate::queue::{TaskQueue, TaskQueueConfig};
use crate::task::{Task, TaskDefinition, TaskId, TaskPriority, TaskStatus};

/// Client for submitting tasks to the distributed task queue
#[derive(Debug)]
pub struct TaskClient {
    queue: Arc<TaskQueue>,
}

impl TaskClient {
    /// Create a new task client with the given queue configuration
    pub async fn new(config: TaskQueueConfig) -> TaskResult<Self> {
        let queue = Arc::new(TaskQueue::new(config).await?);
        Ok(Self { queue })
    }

    /// Create a new task client with default configuration
    pub async fn new_default() -> TaskResult<Self> {
        let queue = Arc::new(TaskQueue::new_default().await?);
        Ok(Self { queue })
    }

    /// Create a task client from an existing queue
    pub fn from_queue(queue: Arc<TaskQueue>) -> Self {
        Self { queue }
    }

    /// Submit a task to the default queue
    pub async fn submit<T>(&self, task: &T) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        self.submit_to_queue(task, "default").await
    }

    /// Submit a task to a specific queue
    pub async fn submit_to_queue<T>(&self, task: &T, queue_name: &str) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        let task_def = TaskDefinition::new(task, queue_name.to_string())?;
        self.queue.submit_task(task_def).await
    }

    /// Submit a task with custom priority
    pub async fn submit_with_priority<T>(
        &self,
        task: &T,
        queue_name: &str,
        priority: TaskPriority,
    ) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        let mut task_def = TaskDefinition::new(task, queue_name.to_string())?;
        task_def.priority = priority;
        self.queue.submit_task(task_def).await
    }

    /// Submit a task to be executed at a specific time
    pub async fn submit_at<T>(
        &self,
        task: &T,
        queue_name: &str,
        scheduled_at: DateTime<Utc>,
    ) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        let task_def = TaskDefinition::new_scheduled(task, queue_name.to_string(), scheduled_at)?;
        self.queue.submit_scheduled_task(task_def).await
    }

    /// Submit a task to be executed after a delay
    pub async fn submit_after<T>(
        &self,
        task: &T,
        queue_name: &str,
        delay_seconds: u64,
    ) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        let scheduled_at = Utc::now() + chrono::Duration::seconds(delay_seconds as i64);
        self.submit_at(task, queue_name, scheduled_at).await
    }

    /// Submit a task with custom configuration
    pub async fn submit_with_config<T>(&self, task_config: TaskSubmissionConfig<'_, T>) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        let mut task_def = TaskDefinition::new(task_config.task, task_config.queue.to_string())?;
        
        if let Some(priority) = task_config.priority {
            task_def.priority = priority;
        }
        
        if let Some(scheduled_at) = task_config.scheduled_at {
            task_def.scheduled_at = Some(scheduled_at);
            task_def.status = TaskStatus::Scheduled;
            self.queue.submit_scheduled_task(task_def).await
        } else {
            self.queue.submit_task(task_def).await
        }
    }

    /// Get task status by ID
    pub async fn get_task_status(&self, task_id: TaskId) -> TaskResult<Option<TaskDefinition>> {
        self.queue.get_task(task_id).await
    }

    /// Wait for a task to complete and return its result
    pub async fn wait_for_result<T>(&self, task_id: TaskId, timeout_seconds: Option<u64>) -> TaskResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let start_time = std::time::Instant::now();
        let timeout = timeout_seconds.map(std::time::Duration::from_secs);

        loop {
            // Check timeout
            if let Some(timeout) = timeout {
                if start_time.elapsed() > timeout {
                    return Err(TaskError::timeout("wait_for_result"));
                }
            }

            // Check task status
            if let Some(task_def) = self.queue.get_task(task_id).await? {
                match task_def.status {
                    TaskStatus::Success => {
                        if let Some(result_json) = task_def.result {
                            let result: T = serde_json::from_str(&result_json)?;
                            return Ok(result);
                        } else {
                            return Err(TaskError::task_execution("Task completed but no result found"));
                        }
                    }
                    TaskStatus::Failed => {
                        let error_msg = task_def.error.unwrap_or_else(|| "Unknown error".to_string());
                        return Err(TaskError::task_execution(error_msg));
                    }
                    TaskStatus::Cancelled => {
                        return Err(TaskError::task_execution("Task was cancelled"));
                    }
                    _ => {
                        // Task is still pending/running, wait and check again
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            } else {
                return Err(TaskError::TaskNotFound {
                    task_id: task_id.to_string(),
                });
            }
        }
    }

    /// Get queue statistics
    pub async fn get_queue_stats(&self, queue_name: &str) -> TaskResult<crate::queue::QueueStats> {
        self.queue.get_stats(queue_name).await
    }

    /// List all available queues
    pub async fn list_queues(&self) -> TaskResult<Vec<String>> {
        self.queue.list_queues().await
    }

    /// Get access to the underlying queue for advanced operations
    pub fn queue(&self) -> &Arc<TaskQueue> {
        &self.queue
    }
}

/// Configuration for task submission
#[derive(Debug)]
pub struct TaskSubmissionConfig<'a, T> {
    /// The task to submit
    pub task: &'a T,
    /// Queue name
    pub queue: &'a str,
    /// Task priority
    pub priority: Option<TaskPriority>,
    /// Scheduled execution time
    pub scheduled_at: Option<DateTime<Utc>>,
}

impl<'a, T> TaskSubmissionConfig<'a, T> {
    /// Create a new task submission configuration
    pub fn new(task: &'a T, queue: &'a str) -> Self {
        Self {
            task,
            queue,
            priority: None,
            scheduled_at: None,
        }
    }

    /// Set task priority
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Set scheduled execution time
    pub fn scheduled_at(mut self, scheduled_at: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(scheduled_at);
        self
    }

    /// Set execution delay in seconds
    pub fn after_delay(mut self, delay_seconds: u64) -> Self {
        self.scheduled_at = Some(Utc::now() + chrono::Duration::seconds(delay_seconds as i64));
        self
    }
}

/// Convenience methods for common task submission patterns
impl TaskClient {
    /// Submit a high-priority task
    pub async fn submit_high_priority<T>(&self, task: &T, queue_name: &str) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        self.submit_with_priority(task, queue_name, TaskPriority::High)
            .await
    }

    /// Submit a critical priority task
    pub async fn submit_critical<T>(&self, task: &T, queue_name: &str) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        self.submit_with_priority(task, queue_name, TaskPriority::Critical)
            .await
    }

    /// Submit a low-priority task
    pub async fn submit_low_priority<T>(&self, task: &T, queue_name: &str) -> TaskResult<TaskId>
    where
        T: Task + Serialize,
    {
        self.submit_with_priority(task, queue_name, TaskPriority::Low)
            .await
    }

    /// Submit and wait for result in one call
    pub async fn submit_and_wait<T, R>(
        &self,
        task: &T,
        queue_name: &str,
        timeout_seconds: Option<u64>,
    ) -> TaskResult<R>
    where
        T: Task + Serialize,
        R: serde::de::DeserializeOwned,
    {
        let task_id = self.submit_to_queue(task, queue_name).await?;
        self.wait_for_result(task_id, timeout_seconds).await
    }

    /// Submit multiple tasks at once
    pub async fn submit_batch<T>(&self, tasks: &[T], queue_name: &str) -> TaskResult<Vec<TaskId>>
    where
        T: Task + Serialize,
    {
        let mut task_ids = Vec::new();
        
        for task in tasks {
            let task_id = self.submit_to_queue(task, queue_name).await?;
            task_ids.push(task_id);
        }
        
        Ok(task_ids)
    }

    /// Submit multiple tasks with different priorities
    pub async fn submit_batch_with_priorities<T>(
        &self,
        tasks: &[(T, TaskPriority)],
        queue_name: &str,
    ) -> TaskResult<Vec<TaskId>>
    where
        T: Task + Serialize,
    {
        let mut task_ids = Vec::new();
        
        for (task, priority) in tasks {
            let task_id = self.submit_with_priority(task, queue_name, priority.clone()).await?;
            task_ids.push(task_id);
        }
        
        Ok(task_ids)
    }
} 