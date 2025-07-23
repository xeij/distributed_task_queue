//! Error types for the distributed task queue

use thiserror::Error;

/// Result type alias for task operations
pub type TaskResult<T> = Result<T, TaskError>;

/// Comprehensive error types for the task queue system
#[derive(Error, Debug)]
pub enum TaskError {
    /// Redis connection or operation errors
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// Task serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Task execution errors
    #[error("Task execution failed: {message}")]
    TaskExecution { message: String },

    /// Task not found in queue
    #[error("Task not found: {task_id}")]
    TaskNotFound { task_id: String },

    /// Queue operation errors
    #[error("Queue operation failed: {operation}: {reason}")]
    QueueOperation { operation: String, reason: String },

    /// Worker errors
    #[error("Worker error: {message}")]
    Worker { message: String },

    /// Scheduler errors
    #[error("Scheduler error: {message}")]
    Scheduler { message: String },

    /// Configuration errors
    #[error("Configuration error: {message}")]
    Config { message: String },

    /// Timeout errors
    #[error("Operation timed out: {operation}")]
    Timeout { operation: String },

    /// Task retry limit exceeded
    #[error("Task retry limit exceeded: {task_id} (max retries: {max_retries})")]
    RetryLimitExceeded { task_id: String, max_retries: u32 },

    /// Generic errors for wrapping other error types
    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl TaskError {
    /// Create a task execution error
    pub fn task_execution<S: Into<String>>(message: S) -> Self {
        Self::TaskExecution {
            message: message.into(),
        }
    }

    /// Create a queue operation error
    pub fn queue_operation<S: Into<String>>(operation: S, reason: S) -> Self {
        Self::QueueOperation {
            operation: operation.into(),
            reason: reason.into(),
        }
    }

    /// Create a worker error
    pub fn worker<S: Into<String>>(message: S) -> Self {
        Self::Worker {
            message: message.into(),
        }
    }

    /// Create a scheduler error
    pub fn scheduler<S: Into<String>>(message: S) -> Self {
        Self::Scheduler {
            message: message.into(),
        }
    }

    /// Create a configuration error
    pub fn config<S: Into<String>>(message: S) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Create a timeout error
    pub fn timeout<S: Into<String>>(operation: S) -> Self {
        Self::Timeout {
            operation: operation.into(),
        }
    }

    /// Check if the error is recoverable (can be retried)
    pub fn is_recoverable(&self) -> bool {
        match self {
            TaskError::Redis(_) => true,
            TaskError::Timeout { .. } => true,
            TaskError::QueueOperation { .. } => true,
            TaskError::Worker { .. } => true,
            TaskError::TaskExecution { .. } => true,
            TaskError::Serialization(_) => false,
            TaskError::TaskNotFound { .. } => false,
            TaskError::Config { .. } => false,
            TaskError::RetryLimitExceeded { .. } => false,
            TaskError::Scheduler { .. } => false,
            TaskError::Internal(_) => false,
            TaskError::Io(_) => true,
        }
    }
} 