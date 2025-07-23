//! Simple worker example
//!
//! This example demonstrates how to:
//! 1. Define custom tasks
//! 2. Set up a worker with task handlers
//! 3. Run the worker to process tasks from the queue
//!
//! To run this example:
//! 1. Make sure Redis is running on localhost:6379
//! 2. Run: cargo run --example simple_worker

use distributed_task_queue::{
    Task, TaskClient, TaskQueue, TaskQueueConfig, Worker, WorkerConfig,
    worker::TaskHandler, TaskResult, TaskError
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber;

/// A simple math task that adds two numbers
#[derive(Debug, Serialize, Deserialize)]
struct AddTask {
    a: i32,
    b: i32,
}

#[async_trait::async_trait]
impl Task for AddTask {
    type Output = i32;
    type Error = TaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        // Simulate some work
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        info!("Adding {} + {} = {}", self.a, self.b, self.a + self.b);
        Ok(self.a + self.b)
    }

    fn name(&self) -> &'static str {
        "AddTask"
    }
}

/// A task that processes a message
#[derive(Debug, Serialize, Deserialize)]
struct MessageTask {
    message: String,
    uppercase: bool,
}

#[async_trait::async_trait]
impl Task for MessageTask {
    type Output = String;
    type Error = TaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        // Simulate some work
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        let result = if self.uppercase {
            self.message.to_uppercase()
        } else {
            self.message.to_lowercase()
        };
        
        info!("Processed message: '{}' -> '{}'", self.message, result);
        Ok(result)
    }

    fn name(&self) -> &'static str {
        "MessageTask"
    }
}

/// A task handler for AddTask
struct AddTaskHandler;

#[async_trait::async_trait]
impl TaskHandler for AddTaskHandler {
    fn can_handle(&self, task_name: &str) -> bool {
        task_name == "AddTask"
    }

    async fn handle(&self, task_data: &str) -> TaskResult<String> {
        let task: AddTask = serde_json::from_str(task_data)?;
        let result = task.execute().await?;
        Ok(serde_json::to_string(&result)?)
    }
}

/// A task handler for MessageTask
struct MessageTaskHandler;

#[async_trait::async_trait]
impl TaskHandler for MessageTaskHandler {
    fn can_handle(&self, task_name: &str) -> bool {
        task_name == "MessageTask"
    }

    async fn handle(&self, task_data: &str) -> TaskResult<String> {
        let task: MessageTask = serde_json::from_str(task_data)?;
        let result = task.execute().await?;
        Ok(serde_json::to_string(&result)?)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("Starting simple worker example");

    // Configure task queue
    let queue_config = TaskQueueConfig {
        redis_url: "redis://127.0.0.1:6379".to_string(),
        default_queue: "examples".to_string(),
        ..Default::default()
    };

    // Create task queue
    let queue = Arc::new(TaskQueue::new(queue_config).await?);

    // Configure worker
    let worker_config = WorkerConfig {
        queues: vec!["examples".to_string(), "math".to_string()],
        max_concurrent_tasks: 2,
        polling_interval_ms: 500,
        ..Default::default()
    };

    // Create worker
    let worker = Worker::new(worker_config, queue.clone());

    // Register task handlers
    worker.register_handler("AddTask".to_string(), AddTaskHandler).await;
    worker.register_handler("MessageTask".to_string(), MessageTaskHandler).await;

    // Submit some test tasks first (you can also run the task_client example)
    let client = TaskClient::from_queue(queue);
    
    info!("Submitting test tasks...");
    
    // Submit some math tasks
    for i in 0..5 {
        let task = AddTask { a: i, b: i * 2 };
        let task_id = client.submit_to_queue(&task, "math").await?;
        info!("Submitted AddTask {}: {}", i, task_id);
    }
    
    // Submit some message tasks
    let messages = vec![
        ("Hello World", true),
        ("Rust is Amazing", false),
        ("Distributed Tasks", true),
    ];
    
    for (msg, uppercase) in messages {
        let task = MessageTask {
            message: msg.to_string(),
            uppercase,
        };
        let task_id = client.submit_to_queue(&task, "examples").await?;
        info!("Submitted MessageTask '{}': {}", msg, task_id);
    }

    info!("Starting worker...");
    
    // Start the worker (this will run until interrupted)
    // In a real application, you might want to handle shutdown signals
    if let Err(e) = worker.start().await {
        eprintln!("Worker error: {}", e);
    }

    Ok(())
}

/// Helper function to submit tasks (can be called from another process)
pub async fn submit_example_tasks() -> Result<(), Box<dyn std::error::Error>> {
    let client = TaskClient::new_default().await?;
    
    // Submit some example tasks
    let add_task = AddTask { a: 10, b: 20 };
    let task_id = client.submit_to_queue(&add_task, "math").await?;
    println!("Submitted task: {}", task_id);
    
    let message_task = MessageTask {
        message: "Hello from client".to_string(),
        uppercase: true,
    };
    let task_id = client.submit_to_queue(&message_task, "examples").await?;
    println!("Submitted task: {}", task_id);
    
    Ok(())
} 