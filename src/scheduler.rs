//! Task scheduler for managing scheduled and periodic tasks

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::client::TaskClient;
use crate::error::{TaskError, TaskResult};
use crate::task::{Task, TaskId, TaskPriority};

/// Unique identifier for scheduled job definitions
pub type ScheduledJobId = Uuid;

/// Cron-like schedule expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScheduleExpression {
    /// Run once at a specific time
    Once(DateTime<Utc>),
    /// Run after a delay in seconds
    Delay(u64),
    /// Run every N seconds
    EverySeconds(u64),
    /// Run every N minutes
    EveryMinutes(u64),
    /// Run every N hours
    EveryHours(u64),
    /// Run daily at a specific time (hour, minute)
    Daily { hour: u32, minute: u32 },
    /// Run weekly on specific day and time (0=Sunday, 1=Monday, etc.)
    Weekly { day: u32, hour: u32, minute: u32 },
    /// Cron expression (basic implementation)
    Cron(String),
}

impl ScheduleExpression {
    /// Calculate the next execution time based on the current time
    pub fn next_execution(&self, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            ScheduleExpression::Once(time) => {
                if *time > from {
                    Some(*time)
                } else {
                    None
                }
            }
            ScheduleExpression::Delay(seconds) => {
                Some(from + Duration::seconds(*seconds as i64))
            }
            ScheduleExpression::EverySeconds(seconds) => {
                Some(from + Duration::seconds(*seconds as i64))
            }
            ScheduleExpression::EveryMinutes(minutes) => {
                Some(from + Duration::minutes(*minutes as i64))
            }
            ScheduleExpression::EveryHours(hours) => {
                Some(from + Duration::hours(*hours as i64))
            }
            ScheduleExpression::Daily { hour, minute } => {
                let mut next = from
                    .date_naive()
                    .and_hms_opt(*hour, *minute, 0)?
                    .and_utc();
                
                if next <= from {
                    next = next + Duration::days(1);
                }
                Some(next)
            }
            ScheduleExpression::Weekly { day, hour, minute } => {
                let current_day = from.weekday().num_days_from_sunday();
                let days_until_target = if *day >= current_day {
                    *day - current_day
                } else {
                    7 - (current_day - *day)
                };
                
                let mut next = from
                    .date_naive()
                    .and_hms_opt(*hour, *minute, 0)?
                    .and_utc()
                    + Duration::days(days_until_target as i64);
                
                if next <= from {
                    next = next + Duration::weeks(1);
                }
                Some(next)
            }
            ScheduleExpression::Cron(_expr) => {
                // Basic cron implementation would go here
                // For now, just return None
                warn!("Cron expressions not fully implemented yet");
                None
            }
        }
    }

    /// Check if this is a recurring schedule
    pub fn is_recurring(&self) -> bool {
        matches!(
            self,
            ScheduleExpression::EverySeconds(_)
                | ScheduleExpression::EveryMinutes(_)
                | ScheduleExpression::EveryHours(_)
                | ScheduleExpression::Daily { .. }
                | ScheduleExpression::Weekly { .. }
                | ScheduleExpression::Cron(_)
        )
    }
}

/// Configuration for a scheduled job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledJob {
    /// Unique job identifier
    pub id: ScheduledJobId,
    /// Job name
    pub name: String,
    /// Task type name
    pub task_type: String,
    /// Serialized task data
    pub task_data: String,
    /// Queue to submit the task to
    pub queue: String,
    /// Task priority
    pub priority: TaskPriority,
    /// Schedule expression
    pub schedule: ScheduleExpression,
    /// Whether the job is enabled
    pub enabled: bool,
    /// Maximum number of retries for failed executions
    pub max_retries: u32,
    /// Next scheduled execution time
    pub next_run: Option<DateTime<Utc>>,
    /// Last execution time
    pub last_run: Option<DateTime<Utc>>,
    /// Number of times this job has been executed
    pub run_count: u64,
    /// Number of failed executions
    pub failure_count: u64,
    /// Job creation time
    pub created_at: DateTime<Utc>,
    /// Job last update time
    pub updated_at: DateTime<Utc>,
}

impl ScheduledJob {
    /// Create a new scheduled job
    pub fn new<T>(
        name: String,
        task: &T,
        queue: String,
        schedule: ScheduleExpression,
    ) -> TaskResult<Self>
    where
        T: Task + Serialize,
    {
        let now = Utc::now();
        let next_run = schedule.next_execution(now);
        
        Ok(Self {
            id: ScheduledJobId::new_v4(),
            name,
            task_type: task.name().to_string(),
            task_data: serde_json::to_string(task)?,
            queue,
            priority: task.priority(),
            schedule,
            enabled: true,
            max_retries: 3,
            next_run,
            last_run: None,
            run_count: 0,
            failure_count: 0,
            created_at: now,
            updated_at: now,
        })
    }

    /// Update the next run time based on the schedule
    pub fn update_next_run(&mut self) {
        let now = Utc::now();
        self.next_run = self.schedule.next_execution(now);
        self.updated_at = now;
    }

    /// Mark job as executed
    pub fn mark_executed(&mut self, success: bool) {
        let now = Utc::now();
        self.last_run = Some(now);
        self.run_count += 1;
        self.updated_at = now;
        
        if !success {
            self.failure_count += 1;
        }
        
        // Update next run time if it's a recurring job
        if self.schedule.is_recurring() {
            self.next_run = self.schedule.next_execution(now);
        } else {
            self.next_run = None;
            self.enabled = false; // Disable one-time jobs after execution
        }
    }

    /// Check if the job is ready to run
    pub fn is_ready(&self) -> bool {
        if !self.enabled {
            return false;
        }
        
        match self.next_run {
            Some(next_run) => Utc::now() >= next_run,
            None => false,
        }
    }
}

/// Task scheduler for managing scheduled and periodic tasks
pub struct TaskScheduler {
    client: Arc<TaskClient>,
    jobs: Arc<RwLock<HashMap<ScheduledJobId, ScheduledJob>>>,
    shutdown_signal: Arc<RwLock<bool>>,
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new(client: Arc<TaskClient>) -> Self {
        Self {
            client,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            shutdown_signal: Arc::new(RwLock::new(false)),
        }
    }

    /// Add a scheduled job
    pub async fn add_job(&self, job: ScheduledJob) -> TaskResult<ScheduledJobId> {
        let job_id = job.id;
        
        info!("Adding scheduled job: {} ({})", job.name, job_id);
        debug!("Job schedule: {:?}", job.schedule);
        
        let mut jobs = self.jobs.write().await;
        jobs.insert(job_id, job);
        
        Ok(job_id)
    }

    /// Remove a scheduled job
    pub async fn remove_job(&self, job_id: ScheduledJobId) -> TaskResult<bool> {
        let mut jobs = self.jobs.write().await;
        let removed = jobs.remove(&job_id).is_some();
        
        if removed {
            info!("Removed scheduled job: {}", job_id);
        }
        
        Ok(removed)
    }

    /// Enable or disable a job
    pub async fn set_job_enabled(&self, job_id: ScheduledJobId, enabled: bool) -> TaskResult<()> {
        let mut jobs = self.jobs.write().await;
        
        if let Some(job) = jobs.get_mut(&job_id) {
            job.enabled = enabled;
            job.updated_at = Utc::now();
            
            info!("Job {} {}", job_id, if enabled { "enabled" } else { "disabled" });
        } else {
            return Err(TaskError::scheduler(format!("Job not found: {}", job_id)));
        }
        
        Ok(())
    }

    /// Get a job by ID
    pub async fn get_job(&self, job_id: ScheduledJobId) -> Option<ScheduledJob> {
        let jobs = self.jobs.read().await;
        jobs.get(&job_id).cloned()
    }

    /// List all jobs
    pub async fn list_jobs(&self) -> Vec<ScheduledJob> {
        let jobs = self.jobs.read().await;
        jobs.values().cloned().collect()
    }

    /// List jobs by status
    pub async fn list_jobs_by_status(&self, enabled: bool) -> Vec<ScheduledJob> {
        let jobs = self.jobs.read().await;
        jobs.values()
            .filter(|job| job.enabled == enabled)
            .cloned()
            .collect()
    }

    /// Start the scheduler
    pub async fn start(&self) -> TaskResult<()> {
        info!("Starting task scheduler");
        
        let mut interval = interval(tokio::time::Duration::from_secs(1));
        
        loop {
            interval.tick().await;
            
            // Check shutdown signal
            if *self.shutdown_signal.read().await {
                break;
            }
            
            // Process ready jobs
            if let Err(e) = self.process_ready_jobs().await {
                error!("Error processing scheduled jobs: {}", e);
            }
        }
        
        info!("Task scheduler stopped");
        Ok(())
    }

    /// Process jobs that are ready to run
    async fn process_ready_jobs(&self) -> TaskResult<()> {
        let ready_jobs = {
            let jobs = self.jobs.read().await;
            jobs.values()
                .filter(|job| job.is_ready())
                .cloned()
                .collect::<Vec<_>>()
        };
        
        for mut job in ready_jobs {
            debug!("Executing scheduled job: {} ({})", job.name, job.id);
            
            // Submit the task
            let result = self.execute_job(&job).await;
            
            // Update job status
            job.mark_executed(result.is_ok());
            
            // Update the job in the collection
            {
                let mut jobs = self.jobs.write().await;
                if job.enabled || job.schedule.is_recurring() {
                    jobs.insert(job.id, job);
                } else {
                    jobs.remove(&job.id);
                }
            }
            
            match result {
                Ok(task_id) => {
                    info!("Scheduled job {} submitted successfully (task: {})", job.name, task_id);
                }
                Err(e) => {
                    error!("Failed to execute scheduled job {}: {}", job.name, e);
                }
            }
        }
        
        Ok(())
    }

    /// Execute a single job
    async fn execute_job(&self, job: &ScheduledJob) -> TaskResult<TaskId> {
        // Parse the task data and submit it
        // Note: In a real implementation, you'd want a registry of task types
        // For now, we'll submit the raw task data
        
        // Create a dummy task for submission
        let task_def = crate::task::TaskDefinition {
            id: crate::task::TaskId::new_v4(),
            name: job.task_type.clone(),
            data: job.task_data.clone(),
            priority: job.priority.clone(),
            status: crate::task::TaskStatus::Pending,
            retry_config: crate::task::RetryConfig::default(),
            retry_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            scheduled_at: None,
            started_at: None,
            finished_at: None,
            result: None,
            error: None,
            queue: job.queue.clone(),
            worker_id: None,
            estimated_duration: None,
        };
        
        self.client.queue().submit_task(task_def).await
    }

    /// Signal the scheduler to shutdown
    pub async fn shutdown(&self) {
        info!("Shutting down task scheduler");
        let mut shutdown = self.shutdown_signal.write().await;
        *shutdown = true;
    }

    /// Get scheduler statistics
    pub async fn get_stats(&self) -> SchedulerStats {
        let jobs = self.jobs.read().await;
        
        let total_jobs = jobs.len();
        let enabled_jobs = jobs.values().filter(|job| job.enabled).count();
        let disabled_jobs = total_jobs - enabled_jobs;
        
        let mut ready_jobs = 0;
        let mut recurring_jobs = 0;
        let mut total_executions = 0;
        let mut total_failures = 0;
        
        for job in jobs.values() {
            if job.is_ready() {
                ready_jobs += 1;
            }
            if job.schedule.is_recurring() {
                recurring_jobs += 1;
            }
            total_executions += job.run_count;
            total_failures += job.failure_count;
        }
        
        SchedulerStats {
            total_jobs,
            enabled_jobs,
            disabled_jobs,
            ready_jobs,
            recurring_jobs,
            total_executions,
            total_failures,
        }
    }
}

/// Scheduler statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerStats {
    pub total_jobs: usize,
    pub enabled_jobs: usize,
    pub disabled_jobs: usize,
    pub ready_jobs: usize,
    pub recurring_jobs: usize,
    pub total_executions: u64,
    pub total_failures: u64,
}

/// Convenience methods for creating scheduled jobs
impl TaskScheduler {
    /// Schedule a task to run once at a specific time
    pub async fn schedule_once<T>(
        &self,
        name: String,
        task: &T,
        queue: String,
        at: DateTime<Utc>,
    ) -> TaskResult<ScheduledJobId>
    where
        T: Task + Serialize,
    {
        let job = ScheduledJob::new(name, task, queue, ScheduleExpression::Once(at))?;
        self.add_job(job).await
    }

    /// Schedule a task to run after a delay
    pub async fn schedule_after<T>(
        &self,
        name: String,
        task: &T,
        queue: String,
        delay_seconds: u64,
    ) -> TaskResult<ScheduledJobId>
    where
        T: Task + Serialize,
    {
        let job = ScheduledJob::new(name, task, queue, ScheduleExpression::Delay(delay_seconds))?;
        self.add_job(job).await
    }

    /// Schedule a task to run every N seconds
    pub async fn schedule_every_seconds<T>(
        &self,
        name: String,
        task: &T,
        queue: String,
        seconds: u64,
    ) -> TaskResult<ScheduledJobId>
    where
        T: Task + Serialize,
    {
        let job = ScheduledJob::new(name, task, queue, ScheduleExpression::EverySeconds(seconds))?;
        self.add_job(job).await
    }

    /// Schedule a task to run every N minutes
    pub async fn schedule_every_minutes<T>(
        &self,
        name: String,
        task: &T,
        queue: String,
        minutes: u64,
    ) -> TaskResult<ScheduledJobId>
    where
        T: Task + Serialize,
    {
        let job = ScheduledJob::new(name, task, queue, ScheduleExpression::EveryMinutes(minutes))?;
        self.add_job(job).await
    }

    /// Schedule a task to run daily
    pub async fn schedule_daily<T>(
        &self,
        name: String,
        task: &T,
        queue: String,
        hour: u32,
        minute: u32,
    ) -> TaskResult<ScheduledJobId>
    where
        T: Task + Serialize,
    {
        let job = ScheduledJob::new(
            name,
            task,
            queue,
            ScheduleExpression::Daily { hour, minute },
        )?;
        self.add_job(job).await
    }

    /// Schedule a task to run weekly
    pub async fn schedule_weekly<T>(
        &self,
        name: String,
        task: &T,
        queue: String,
        day: u32,
        hour: u32,
        minute: u32,
    ) -> TaskResult<ScheduledJobId>
    where
        T: Task + Serialize,
    {
        let job = ScheduledJob::new(
            name,
            task,
            queue,
            ScheduleExpression::Weekly { day, hour, minute },
        )?;
        self.add_job(job).await
    }
} 