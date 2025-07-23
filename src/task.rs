//! Task definitions and management

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

use crate::error::{TaskError, TaskResult};

/// Unique identifier for tasks
pub type TaskId = Uuid;

/// Task execution status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is waiting to be processed
    Pending,
    /// Task is currently being processed
    Running,
    /// Task completed successfully
    Success,
    /// Task failed with an error
    Failed,
    /// Task was cancelled
    Cancelled,
    /// Task is scheduled for future execution
    Scheduled,
    /// Task is being retried
    Retrying,
}

/// Task priority levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Low = 0,
    Normal = 5,
    High = 10,
    Critical = 15,
}

impl Default for TaskPriority {
    fn default() -> Self {
        TaskPriority::Normal
    }
}

/// Configuration for task retry behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Base delay between retries in seconds
    pub retry_delay: u64,
    /// Whether to use exponential backoff
    pub exponential_backoff: bool,
    /// Maximum delay between retries in seconds
    pub max_delay: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay: 5,
            exponential_backoff: true,
            max_delay: 300, // 5 minutes
        }
    }
}

/// Core trait that all tasks must implement
#[async_trait]
pub trait Task: Send + Sync + Debug {
    /// The output type of the task
    type Output: Send + Sync + Serialize + for<'de> Deserialize<'de>;
    /// The error type for task execution
    type Error: Send + Sync + std::error::Error + 'static;

    /// Execute the task and return the result
    async fn execute(&self) -> Result<Self::Output, Self::Error>;

    /// Get the task name (defaults to the type name)
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Get the retry configuration for this task
    fn retry_config(&self) -> RetryConfig {
        RetryConfig::default()
    }

    /// Get the task priority
    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    /// Estimate task execution time in seconds (for scheduling)
    fn estimated_duration(&self) -> Option<u64> {
        None
    }
}

/// Complete task definition with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDefinition {
    /// Unique task identifier
    pub id: TaskId,
    /// Task name/type
    pub name: String,
    /// Serialized task data
    pub data: String,
    /// Task priority
    pub priority: TaskPriority,
    /// Current task status
    pub status: TaskStatus,
    /// Retry configuration
    pub retry_config: RetryConfig,
    /// Current retry attempt
    pub retry_count: u32,
    /// When the task was created
    pub created_at: DateTime<Utc>,
    /// When the task was last updated
    pub updated_at: DateTime<Utc>,
    /// When the task should be executed (for scheduled tasks)
    pub scheduled_at: Option<DateTime<Utc>>,
    /// When the task started executing
    pub started_at: Option<DateTime<Utc>>,
    /// When the task finished executing
    pub finished_at: Option<DateTime<Utc>>,
    /// Task execution result (if completed)
    pub result: Option<String>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Queue name
    pub queue: String,
    /// Worker ID that processed the task
    pub worker_id: Option<String>,
    /// Estimated execution duration
    pub estimated_duration: Option<u64>,
}

impl TaskDefinition {
    /// Create a new task definition
    pub fn new<T>(task: &T, queue: String) -> TaskResult<Self>
    where
        T: Task + Serialize,
    {
        let now = Utc::now();
        Ok(Self {
            id: TaskId::new_v4(),
            name: task.name().to_string(),
            data: serde_json::to_string(task)?,
            priority: task.priority(),
            status: TaskStatus::Pending,
            retry_config: task.retry_config(),
            retry_count: 0,
            created_at: now,
            updated_at: now,
            scheduled_at: None,
            started_at: None,
            finished_at: None,
            result: None,
            error: None,
            queue,
            worker_id: None,
            estimated_duration: task.estimated_duration(),
        })
    }

    /// Create a scheduled task definition
    pub fn new_scheduled<T>(
        task: &T,
        queue: String,
        scheduled_at: DateTime<Utc>,
    ) -> TaskResult<Self>
    where
        T: Task + Serialize,
    {
        let mut task_def = Self::new(task, queue)?;
        task_def.status = TaskStatus::Scheduled;
        task_def.scheduled_at = Some(scheduled_at);
        Ok(task_def)
    }

    /// Mark task as started
    pub fn mark_started(&mut self, worker_id: String) {
        self.status = TaskStatus::Running;
        self.started_at = Some(Utc::now());
        self.updated_at = Utc::now();
        self.worker_id = Some(worker_id);
    }

    /// Mark task as completed successfully
    pub fn mark_success<T>(&mut self, result: &T) -> TaskResult<()>
    where
        T: Serialize,
    {
        self.status = TaskStatus::Success;
        self.finished_at = Some(Utc::now());
        self.updated_at = Utc::now();
        self.result = Some(serde_json::to_string(result)?);
        Ok(())
    }

    /// Mark task as failed
    pub fn mark_failed(&mut self, error: &str) {
        self.status = TaskStatus::Failed;
        self.finished_at = Some(Utc::now());
        self.updated_at = Utc::now();
        self.error = Some(error.to_string());
    }

    /// Mark task for retry
    pub fn mark_retry(&mut self) -> TaskResult<()> {
        if self.retry_count >= self.retry_config.max_retries {
            return Err(TaskError::RetryLimitExceeded {
                task_id: self.id.to_string(),
                max_retries: self.retry_config.max_retries,
            });
        }

        self.retry_count += 1;
        self.status = TaskStatus::Retrying;
        self.updated_at = Utc::now();
        self.started_at = None;
        self.finished_at = None;
        self.worker_id = None;

        // Calculate next retry time with exponential backoff
        let delay = if self.retry_config.exponential_backoff {
            let exponential_delay = self.retry_config.retry_delay * (2_u64.pow(self.retry_count - 1));
            exponential_delay.min(self.retry_config.max_delay)
        } else {
            self.retry_config.retry_delay
        };

        self.scheduled_at = Some(Utc::now() + chrono::Duration::seconds(delay as i64));
        Ok(())
    }

    /// Check if task can be retried
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.retry_config.max_retries
    }

    /// Check if task is ready to be executed (for scheduled tasks)
    pub fn is_ready(&self) -> bool {
        match self.scheduled_at {
            Some(scheduled_at) => Utc::now() >= scheduled_at,
            None => true,
        }
    }

    /// Get task execution duration if available
    pub fn execution_duration(&self) -> Option<chrono::Duration> {
        match (self.started_at, self.finished_at) {
            (Some(started), Some(finished)) => Some(finished - started),
            _ => None,
        }
    }
} 