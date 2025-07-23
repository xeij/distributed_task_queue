//! Task client example
//!
//! This example demonstrates how to:
//! 1. Submit tasks to the queue
//! 2. Wait for task results
//! 3. Submit tasks with different priorities
//! 4. Submit tasks in batches
//! 5. Use different queue configurations
//!
//! To run this example:
//! 1. Make sure Redis is running on localhost:6379
//! 2. Make sure a worker is running (run simple_worker example)
//! 3. Run: cargo run --example task_client

use distributed_task_queue::{
    Task, TaskClient, TaskPriority, TaskResult, TaskError
};
use serde::{Deserialize, Serialize};
use tracing::{info, Level};
use tracing_subscriber;

/// A simple computation task
#[derive(Debug, Serialize, Deserialize)]
struct ComputeTask {
    operation: String,
    x: f64,
    y: f64,
}

#[async_trait::async_trait]
impl Task for ComputeTask {
    type Output = f64;
    type Error = TaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        match self.operation.as_str() {
            "add" => Ok(self.x + self.y),
            "subtract" => Ok(self.x - self.y),
            "multiply" => Ok(self.x * self.y),
            "divide" => {
                if self.y == 0.0 {
                    Err(TaskError::task_execution("Division by zero"))
                } else {
                    Ok(self.x / self.y)
                }
            }
            _ => Err(TaskError::task_execution(format!("Unknown operation: {}", self.operation))),
        }
    }

    fn name(&self) -> &'static str {
        "ComputeTask"
    }

    fn priority(&self) -> TaskPriority {
        match self.operation.as_str() {
            "divide" => TaskPriority::High,  // Division might be more critical
            "multiply" => TaskPriority::Normal,
            _ => TaskPriority::Low,
        }
    }
}

/// A long-running task
#[derive(Debug, Serialize, Deserialize)]
struct LongTask {
    duration_seconds: u64,
    result_value: i32,
}

#[async_trait::async_trait]
impl Task for LongTask {
    type Output = i32;
    type Error = TaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        tokio::time::sleep(tokio::time::Duration::from_secs(self.duration_seconds)).await;
        Ok(self.result_value)
    }

    fn name(&self) -> &'static str {
        "LongTask"
    }

    fn estimated_duration(&self) -> Option<u64> {
        Some(self.duration_seconds)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("Starting task client example");

    // Create task client
    let client = TaskClient::new_default().await?;

    // Example 1: Submit and wait for a simple task
    info!("=== Example 1: Simple task submission ===");
    let task = ComputeTask {
        operation: "add".to_string(),
        x: 10.0,
        y: 20.0,
    };

    let task_id = client.submit_to_queue(&task, "math").await?;
    info!("Submitted task: {}", task_id);

    // Wait for result with timeout
    match client.wait_for_result::<f64>(task_id, Some(30)).await {
        Ok(result) => info!("Task result: {}", result),
        Err(e) => info!("Task failed: {}", e),
    }

    // Example 2: Submit and wait in one call
    info!("=== Example 2: Submit and wait ===");
    let task = ComputeTask {
        operation: "multiply".to_string(),
        x: 5.0,
        y: 6.0,
    };

    match client.submit_and_wait::<_, f64>(&task, "math", Some(30)).await {
        Ok(result) => info!("Multiply result: {}", result),
        Err(e) => info!("Multiply task failed: {}", e),
    }

    // Example 3: Submit tasks with different priorities
    info!("=== Example 3: Priority tasks ===");
    let tasks = vec![
        ComputeTask { operation: "add".to_string(), x: 1.0, y: 1.0 },
        ComputeTask { operation: "divide".to_string(), x: 100.0, y: 10.0 },
        ComputeTask { operation: "subtract".to_string(), x: 50.0, y: 25.0 },
    ];

    for task in &tasks {
        let task_id = match task.priority() {
            TaskPriority::High => client.submit_high_priority(task, "math").await?,
            TaskPriority::Low => client.submit_low_priority(task, "math").await?,
            _ => client.submit_to_queue(task, "math").await?,
        };
        info!("Submitted {} task (priority: {:?}): {}", task.operation, task.priority(), task_id);
    }

    // Example 4: Batch submission
    info!("=== Example 4: Batch submission ===");
    let batch_tasks = vec![
        ComputeTask { operation: "add".to_string(), x: 1.0, y: 2.0 },
        ComputeTask { operation: "add".to_string(), x: 3.0, y: 4.0 },
        ComputeTask { operation: "add".to_string(), x: 5.0, y: 6.0 },
    ];

    let task_ids = client.submit_batch(&batch_tasks, "math").await?;
    info!("Submitted batch of {} tasks", task_ids.len());

    // Wait for all batch results
    for task_id in task_ids {
        match client.wait_for_result::<f64>(task_id, Some(10)).await {
            Ok(result) => info!("Batch task {} result: {}", task_id, result),
            Err(e) => info!("Batch task {} failed: {}", task_id, e),
        }
    }

    // Example 5: Long-running task with timeout
    info!("=== Example 5: Long-running task ===");
    let long_task = LongTask {
        duration_seconds: 3,
        result_value: 42,
    };

    let task_id = client.submit_to_queue(&long_task, "slow").await?;
    info!("Submitted long-running task: {}", task_id);

    // Try with short timeout (should timeout)
    match client.wait_for_result::<i32>(task_id, Some(1)).await {
        Ok(result) => info!("Long task result (unexpected): {}", result),
        Err(e) => info!("Long task timed out as expected: {}", e),
    }

    // Wait with longer timeout
    match client.wait_for_result::<i32>(task_id, Some(10)).await {
        Ok(result) => info!("Long task result: {}", result),
        Err(e) => info!("Long task failed: {}", e),
    }

    // Example 6: Task status checking
    info!("=== Example 6: Task status checking ===");
    let task = ComputeTask {
        operation: "divide".to_string(),
        x: 15.0,
        y: 3.0,
    };

    let task_id = client.submit_to_queue(&task, "math").await?;
    info!("Submitted task for status checking: {}", task_id);

    // Check status periodically
    for i in 0..5 {
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        
        if let Some(task_def) = client.get_task_status(task_id).await? {
            info!("Check {}: Task status: {:?}", i + 1, task_def.status);
            
            if matches!(task_def.status, 
                distributed_task_queue::TaskStatus::Success | 
                distributed_task_queue::TaskStatus::Failed) {
                if let Some(result) = task_def.result {
                    info!("Final result: {}", result);
                }
                break;
            }
        }
    }

    // Example 7: Queue statistics
    info!("=== Example 7: Queue statistics ===");
    let stats = client.get_queue_stats("math").await?;
    info!("Math queue stats: {:?}", stats);

    let queues = client.list_queues().await?;
    info!("Available queues: {:?}", queues);

    // Example 8: Error handling
    info!("=== Example 8: Error handling ===");
    let error_task = ComputeTask {
        operation: "divide".to_string(),
        x: 10.0,
        y: 0.0,  // This will cause division by zero
    };

    match client.submit_and_wait::<_, f64>(&error_task, "math", Some(10)).await {
        Ok(result) => info!("Unexpected success: {}", result),
        Err(e) => info!("Expected error caught: {}", e),
    }

    info!("Task client example completed!");
    Ok(())
} 