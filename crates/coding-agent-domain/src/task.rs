use crate::{ClientRequestId, DomainError, EventId, RepositoryId, TaskId, UtcTimestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl TaskStatus {
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running)
                | (Self::Queued, Self::Cancelled)
                | (Self::Queued, Self::Interrupted)
                | (Self::Running, Self::Completed)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Cancelled)
                | (Self::Running, Self::Interrupted)
        )
    }

    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NewTask {
    pub client_request_id: ClientRequestId,
    pub repository_id: RepositoryId,
    pub prompt: String,
}

impl NewTask {
    pub fn try_new(
        client_request_id: ClientRequestId,
        repository_id: RepositoryId,
        prompt: impl Into<String>,
    ) -> Result<Self, DomainError> {
        let prompt = prompt.into();
        let prompt = prompt.trim();
        let scalar_count = prompt.chars().count();

        if scalar_count == 0 || scalar_count > 50_000 {
            return Err(DomainError::InvalidPrompt);
        }

        Ok(Self {
            client_request_id,
            repository_id,
            prompt: prompt.to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub client_request_id: ClientRequestId,
    pub repository_id: RepositoryId,
    pub prompt: String,
    pub status: TaskStatus,
    pub attempt: u32,
    pub retry_of: Option<TaskId>,
    pub created_at: UtcTimestamp,
    pub started_at: Option<UtcTimestamp>,
    pub finished_at: Option<UtcTimestamp>,
    pub last_event_id: EventId,
    pub failure: Option<TaskFailure>,
}

impl Task {
    pub fn try_from_stored(task: Self) -> Result<Self, DomainError> {
        if task.attempt == 0 {
            return Err(DomainError::InvalidTaskAttempt);
        }

        let state_is_valid = match task.status {
            TaskStatus::Queued => {
                task.started_at.is_none() && task.finished_at.is_none() && task.failure.is_none()
            }
            TaskStatus::Running => {
                task.started_at.is_some() && task.finished_at.is_none() && task.failure.is_none()
            }
            TaskStatus::Completed => {
                task.started_at.is_some() && task.finished_at.is_some() && task.failure.is_none()
            }
            TaskStatus::Failed => {
                task.started_at.is_some() && task.finished_at.is_some() && task.failure.is_some()
            }
            TaskStatus::Cancelled => task.finished_at.is_some() && task.failure.is_none(),
            TaskStatus::Interrupted => task.finished_at.is_some() && task.failure.is_some(),
        };

        if !state_is_valid {
            return Err(DomainError::InvalidTaskState);
        }

        Ok(task)
    }
}
