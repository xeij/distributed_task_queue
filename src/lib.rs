//! # Distributed Task Queue
//!
//! A lightweight distributed task queue similar to Celery, using Rust and Redis for job scheduling.
//!
//! ## Features
//!
//! - Async task execution
//! - Redis-backed job queue
//! - Task scheduling and delays
//! - Worker scaling
//! - Task retry mechanisms
//! - Result storage
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use distributed_task_queue::{TaskQueue, Worker, Task};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyTask {
//!     message: String,
//! }
//!
//! #[async_trait::async_trait]
//! impl Task for MyTask {
//!     type Output = String;
//!     type Error = anyhow::Error;
//!
//!     async fn execute(&self) -> Result<Self::Output, Self::Error> {
//!         Ok(format!("Processed: {}", self.message))
//!     }
//! }
//! ```

pub mod client;
pub mod error;
pub mod queue;
pub mod scheduler;
pub mod task;
pub mod worker;

// Re-export commonly used types
pub use client::TaskClient;
pub use error::{TaskError, TaskResult};
pub use queue::TaskQueue;
pub use scheduler::TaskScheduler;
pub use task::{Task, TaskDefinition, TaskId, TaskStatus};
pub use worker::{Worker, WorkerConfig};

/// Version of the distributed task queue library
pub const VERSION: &str = env!("CARGO_PKG_VERSION"); 