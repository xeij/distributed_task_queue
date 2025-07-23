//! Scheduled tasks example
//!
//! This example demonstrates how to:
//! 1. Schedule tasks to run at specific times
//! 2. Schedule recurring tasks
//! 3. Manage scheduled jobs
//! 4. Use different schedule expressions
//!
//! To run this example:
//! 1. Make sure Redis is running on localhost:6379
//! 2. Run: cargo run --example scheduled_tasks

use distributed_task_queue::{
    Task, TaskClient, TaskScheduler, 
    scheduler::{ScheduleExpression, ScheduledJob},
    TaskResult, TaskError
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber;

/// A task that logs a message
#[derive(Debug, Serialize, Deserialize)]
struct LogTask {
    message: String,
    level: String,
}

#[async_trait::async_trait]
impl Task for LogTask {
    type Output = String;
    type Error = TaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
        let log_message = format!("[{}] {}: {}", timestamp, self.level, self.message);
        
        match self.level.as_str() {
            "INFO" => info!("{}", log_message),
            "WARN" => tracing::warn!("{}", log_message),
            "ERROR" => tracing::error!("{}", log_message),
            _ => info!("{}", log_message),
        }
        
        Ok(log_message)
    }

    fn name(&self) -> &'static str {
        "LogTask"
    }
}

/// A cleanup task
#[derive(Debug, Serialize, Deserialize)]
struct CleanupTask {
    target: String,
    dry_run: bool,
}

#[async_trait::async_trait]
impl Task for CleanupTask {
    type Output = String;
    type Error = TaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        let action = if self.dry_run { "Would clean" } else { "Cleaning" };
        let result = format!("{} target: {}", action, self.target);
        
        info!("{}", result);
        
        // Simulate cleanup work
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        
        Ok(result)
    }

    fn name(&self) -> &'static str {
        "CleanupTask"
    }
}

/// A report generation task
#[derive(Debug, Serialize, Deserialize)]
struct ReportTask {
    report_type: String,
    start_date: String,
    end_date: String,
}

#[async_trait::async_trait]
impl Task for ReportTask {
    type Output = String;
    type Error = TaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        let result = format!(
            "Generated {} report for period {} to {}",
            self.report_type, self.start_date, self.end_date
        );
        
        info!("{}", result);
        
        // Simulate report generation
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        Ok(result)
    }

    fn name(&self) -> &'static str {
        "ReportTask"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("Starting scheduled tasks example");

    // Create task client and scheduler
    let client = Arc::new(TaskClient::new_default().await?);
    let scheduler = TaskScheduler::new(client.clone());

    // Example 1: Schedule a one-time task
    info!("=== Example 1: One-time scheduled task ===");
    let log_task = LogTask {
        message: "This is a scheduled log message".to_string(),
        level: "INFO".to_string(),
    };

    let scheduled_time = Utc::now() + Duration::seconds(5);
    let job_id = scheduler.schedule_once(
        "Welcome Log".to_string(),
        &log_task,
        "logs".to_string(),
        scheduled_time,
    ).await?;
    
    info!("Scheduled one-time task {} to run at {}", job_id, scheduled_time);

    // Example 2: Schedule recurring tasks
    info!("=== Example 2: Recurring tasks ===");
    
    // Every 10 seconds
    let heartbeat_task = LogTask {
        message: "System heartbeat".to_string(),
        level: "INFO".to_string(),
    };
    
    let heartbeat_job = scheduler.schedule_every_seconds(
        "Heartbeat".to_string(),
        &heartbeat_task,
        "system".to_string(),
        10,
    ).await?;
    
    info!("Scheduled heartbeat task {} to run every 10 seconds", heartbeat_job);

    // Every 2 minutes
    let status_task = LogTask {
        message: "System status check".to_string(),
        level: "INFO".to_string(),
    };
    
    let status_job = scheduler.schedule_every_minutes(
        "Status Check".to_string(),
        &status_task,
        "system".to_string(),
        2,
    ).await?;
    
    info!("Scheduled status check task {} to run every 2 minutes", status_job);

    // Example 3: Daily cleanup task
    info!("=== Example 3: Daily scheduled task ===");
    let cleanup_task = CleanupTask {
        target: "/tmp/cache".to_string(),
        dry_run: false,
    };
    
    let cleanup_job = scheduler.schedule_daily(
        "Daily Cleanup".to_string(),
        &cleanup_task,
        "maintenance".to_string(),
        2,  // 2 AM
        30, // 30 minutes
    ).await?;
    
    info!("Scheduled daily cleanup task {} at 02:30", cleanup_job);

    // Example 4: Weekly report
    info!("=== Example 4: Weekly scheduled task ===");
    let report_task = ReportTask {
        report_type: "Weekly Summary".to_string(),
        start_date: "2024-01-01".to_string(),
        end_date: "2024-01-07".to_string(),
    };
    
    let report_job = scheduler.schedule_weekly(
        "Weekly Report".to_string(),
        &report_task,
        "reports".to_string(),
        1,  // Monday
        9,  // 9 AM
        0,  // 0 minutes
    ).await?;
    
    info!("Scheduled weekly report task {} for Mondays at 09:00", report_job);

    // Example 5: Custom schedule expressions
    info!("=== Example 5: Custom schedule expressions ===");
    
    // Task that runs after a delay
    let delayed_task = LogTask {
        message: "This task was delayed".to_string(),
        level: "WARN".to_string(),
    };
    
    let delayed_job = ScheduledJob::new(
        "Delayed Task".to_string(),
        &delayed_task,
        "system".to_string(),
        ScheduleExpression::Delay(15), // 15 seconds delay
    )?;
    
    let delayed_job_id = scheduler.add_job(delayed_job).await?;
    info!("Added delayed task {} (15 seconds delay)", delayed_job_id);

    // Example 6: Managing scheduled jobs
    info!("=== Example 6: Job management ===");
    
    // List all jobs
    let jobs = scheduler.list_jobs().await;
    info!("Total scheduled jobs: {}", jobs.len());
    
    for job in &jobs {
        info!("Job: {} - {} (enabled: {}, next run: {:?})", 
              job.id, job.name, job.enabled, job.next_run);
    }

    // Get scheduler statistics
    let stats = scheduler.get_stats().await;
    info!("Scheduler stats: {:?}", stats);

    // Example 7: Disable and re-enable a job
    info!("=== Example 7: Enable/Disable jobs ===");
    
    // Disable the heartbeat job temporarily
    scheduler.set_job_enabled(heartbeat_job, false).await?;
    info!("Disabled heartbeat job");
    
    // Wait a bit
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    // Re-enable it
    scheduler.set_job_enabled(heartbeat_job, true).await?;
    info!("Re-enabled heartbeat job");

    // Example 8: Start the scheduler and run for a while
    info!("=== Example 8: Running scheduler ===");
    info!("Starting scheduler... (will run for 60 seconds)");
    
    // Start scheduler in background
    let scheduler_handle = {
        let scheduler = Arc::new(scheduler);
        let scheduler_clone = scheduler.clone();
        
        tokio::spawn(async move {
            if let Err(e) = scheduler_clone.start().await {
                tracing::error!("Scheduler error: {}", e);
            }
        })
    };
    
    // Let it run for a while to see the scheduled tasks execute
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    
    info!("Shutting down scheduler...");
    scheduler_handle.abort();
    
    // Final stats
    let final_stats = scheduler.get_stats().await;
    info!("Final scheduler stats: {:?}", final_stats);

    info!("Scheduled tasks example completed!");
    Ok(())
}

/// Example of creating a custom schedule expression
fn create_custom_schedule() -> ScheduleExpression {
    // Run every 5 minutes
    ScheduleExpression::EveryMinutes(5)
}

/// Example of a more complex scheduled job setup
async fn setup_maintenance_jobs(scheduler: &TaskScheduler) -> Result<(), Box<dyn std::error::Error>> {
    // Database backup every day at 3 AM
    let backup_task = LogTask {
        message: "Running database backup".to_string(),
        level: "INFO".to_string(),
    };
    
    scheduler.schedule_daily(
        "Database Backup".to_string(),
        &backup_task,
        "maintenance".to_string(),
        3,
        0,
    ).await?;

    // Log rotation every Sunday at midnight
    let rotation_task = LogTask {
        message: "Rotating log files".to_string(),
        level: "INFO".to_string(),
    };
    
    scheduler.schedule_weekly(
        "Log Rotation".to_string(),
        &rotation_task,
        "maintenance".to_string(),
        0, // Sunday
        0, // Midnight
        0,
    ).await?;

    // System health check every 30 minutes
    let health_task = LogTask {
        message: "System health check".to_string(),
        level: "INFO".to_string(),
    };
    
    scheduler.schedule_every_minutes(
        "Health Check".to_string(),
        &health_task,
        "monitoring".to_string(),
        30,
    ).await?;

    Ok(())
} 