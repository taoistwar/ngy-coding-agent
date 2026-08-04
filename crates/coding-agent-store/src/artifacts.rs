use std::fmt;
use std::path::PathBuf;

use coding_agent_domain::{CanonicalPath, RepositoryId, TaskId, UtcTimestamp};
use time::OffsetDateTime;

use crate::{Store, StoreError};

type ArtifactRecord = (
    String,
    String,
    i64,
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptArtifactState {
    Reserved,
    Ready,
    Inconsistent,
}

impl AttemptArtifactState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Ready => "ready",
            Self::Inconsistent => "inconsistent",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "reserved" => Ok(Self::Reserved),
            "ready" => Ok(Self::Ready),
            "inconsistent" => Ok(Self::Inconsistent),
            _ => Err(StoreError::InvalidArtifactState(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptArtifactIdentity {
    pub task_id: TaskId,
    pub repository_id: RepositoryId,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveAttemptArtifact {
    pub identity: AttemptArtifactIdentity,
    pub base_commit: String,
    pub branch_name: String,
    pub worktree_path: CanonicalPath,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TaskAttemptArtifact {
    pub identity: AttemptArtifactIdentity,
    pub base_commit: String,
    pub branch_name: String,
    pub worktree_path: CanonicalPath,
    pub state: AttemptArtifactState,
    pub failure_code: Option<String>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl fmt::Debug for TaskAttemptArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskAttemptArtifact")
            .field("identity", &self.identity)
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReserveAttemptArtifactOutcome {
    Created(TaskAttemptArtifact),
    Existing(TaskAttemptArtifact),
}

impl ReserveAttemptArtifactOutcome {
    pub const fn artifact(&self) -> &TaskAttemptArtifact {
        match self {
            Self::Created(artifact) | Self::Existing(artifact) => artifact,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateAttemptArtifactOutcome {
    Applied(TaskAttemptArtifact),
    Unchanged(TaskAttemptArtifact),
}

impl UpdateAttemptArtifactOutcome {
    pub const fn artifact(&self) -> &TaskAttemptArtifact {
        match self {
            Self::Applied(artifact) | Self::Unchanged(artifact) => artifact,
        }
    }
}

impl Store {
    pub async fn reserve_attempt_artifact(
        &self,
        input: ReserveAttemptArtifact,
    ) -> Result<ReserveAttemptArtifactOutcome, StoreError> {
        validate_reservation(&input)?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let task_identity: Option<(String, i64)> =
            sqlx::query_as("SELECT repository_id, attempt FROM tasks WHERE id = ?")
                .bind(input.identity.task_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?;
        let Some((repository_id, attempt)) = task_identity else {
            return Err(StoreError::TaskNotFound);
        };
        if repository_id != input.identity.repository_id.to_string()
            || attempt != i64::from(input.identity.attempt)
        {
            return Err(StoreError::ArtifactIdentityConflict);
        }

        if let Some(existing) = load_artifact(&mut *transaction, input.identity.task_id).await? {
            if reservation_matches(&existing, &input) {
                transaction.commit().await?;
                return Ok(ReserveAttemptArtifactOutcome::Existing(existing));
            }
            return Err(StoreError::ArtifactIdentityConflict);
        }

        let conflicting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_attempt_artifacts \
             WHERE branch_name = ? OR worktree_path = ?",
        )
        .bind(&input.branch_name)
        .bind(input.worktree_path.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if conflicting != 0 {
            return Err(StoreError::ArtifactIdentityConflict);
        }

        let now = current_timestamp()?;
        sqlx::query(
            "INSERT INTO task_attempt_artifacts (\
                 task_id, repository_id, attempt, base_commit, branch_name, worktree_path,\
                 state, failure_code, created_at, updated_at\
             ) VALUES (?, ?, ?, ?, ?, ?, 'reserved', NULL, ?, ?)",
        )
        .bind(input.identity.task_id.to_string())
        .bind(input.identity.repository_id.to_string())
        .bind(i64::from(input.identity.attempt))
        .bind(&input.base_commit)
        .bind(&input.branch_name)
        .bind(input.worktree_path.to_string())
        .bind(now.to_string())
        .bind(now.to_string())
        .execute(&mut *transaction)
        .await?;

        let artifact = load_artifact(&mut *transaction, input.identity.task_id)
            .await?
            .ok_or(StoreError::InvariantViolation(
                "inserted attempt artifact is missing",
            ))?;
        transaction.commit().await?;
        Ok(ReserveAttemptArtifactOutcome::Created(artifact))
    }

    pub async fn mark_attempt_artifact_ready(
        &self,
        identity: AttemptArtifactIdentity,
    ) -> Result<UpdateAttemptArtifactOutcome, StoreError> {
        self.transition_attempt_artifact(identity, AttemptArtifactState::Ready, None)
            .await
    }

    pub async fn mark_attempt_artifact_inconsistent(
        &self,
        identity: AttemptArtifactIdentity,
        failure_code: impl Into<String>,
    ) -> Result<UpdateAttemptArtifactOutcome, StoreError> {
        let failure_code = failure_code.into();
        if failure_code.is_empty() {
            return Err(StoreError::InvalidArtifactInput);
        }
        self.transition_attempt_artifact(
            identity,
            AttemptArtifactState::Inconsistent,
            Some(failure_code),
        )
        .await
    }

    pub async fn load_attempt_artifact(
        &self,
        task_id: TaskId,
    ) -> Result<Option<TaskAttemptArtifact>, StoreError> {
        let mut connection = self.pool.acquire().await?;
        load_artifact(&mut *connection, task_id).await
    }

    pub async fn list_reserved_attempt_artifacts(
        &self,
    ) -> Result<Vec<TaskAttemptArtifact>, StoreError> {
        let records: Vec<ArtifactRecord> = sqlx::query_as(
            "SELECT task_id, repository_id, attempt, base_commit, branch_name, worktree_path, \
                    state, failure_code, created_at, updated_at \
             FROM task_attempt_artifacts WHERE state = 'reserved' \
             ORDER BY created_at, task_id",
        )
        .fetch_all(&self.pool)
        .await?;
        records.into_iter().map(artifact_from_record).collect()
    }

    async fn transition_attempt_artifact(
        &self,
        identity: AttemptArtifactIdentity,
        next: AttemptArtifactState,
        failure_code: Option<String>,
    ) -> Result<UpdateAttemptArtifactOutcome, StoreError> {
        if identity.attempt == 0 || next == AttemptArtifactState::Reserved {
            return Err(StoreError::InvalidArtifactInput);
        }
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current = load_artifact(&mut *transaction, identity.task_id)
            .await?
            .ok_or(StoreError::ArtifactNotFound)?;
        if current.identity != identity {
            return Err(StoreError::ArtifactIdentityConflict);
        }
        if current.state == next && current.failure_code == failure_code {
            transaction.commit().await?;
            return Ok(UpdateAttemptArtifactOutcome::Unchanged(current));
        }
        if current.state != AttemptArtifactState::Reserved {
            return Err(StoreError::ArtifactStateConflict);
        }

        let now = current_timestamp()?;
        let changed = sqlx::query(
            "UPDATE task_attempt_artifacts SET state = ?, failure_code = ?, updated_at = ? \
             WHERE task_id = ? AND repository_id = ? AND attempt = ? AND state = 'reserved'",
        )
        .bind(next.as_str())
        .bind(&failure_code)
        .bind(now.to_string())
        .bind(identity.task_id.to_string())
        .bind(identity.repository_id.to_string())
        .bind(i64::from(identity.attempt))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(StoreError::ArtifactStateConflict);
        }
        let artifact = load_artifact(&mut *transaction, identity.task_id)
            .await?
            .ok_or(StoreError::InvariantViolation(
                "updated attempt artifact is missing",
            ))?;
        transaction.commit().await?;
        Ok(UpdateAttemptArtifactOutcome::Applied(artifact))
    }
}

pub(crate) async fn load_artifact<'e, E>(
    executor: E,
    task_id: TaskId,
) -> Result<Option<TaskAttemptArtifact>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let record: Option<ArtifactRecord> = sqlx::query_as(
        "SELECT task_id, repository_id, attempt, base_commit, branch_name, worktree_path, \
                state, failure_code, created_at, updated_at \
         FROM task_attempt_artifacts WHERE task_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_optional(executor)
    .await?;
    record.map(artifact_from_record).transpose()
}

fn artifact_from_record(record: ArtifactRecord) -> Result<TaskAttemptArtifact, StoreError> {
    let attempt = u32::try_from(record.2).map_err(|_| StoreError::TaskAttemptOverflow)?;
    if attempt == 0 {
        return Err(StoreError::TaskAttemptOverflow);
    }
    Ok(TaskAttemptArtifact {
        identity: AttemptArtifactIdentity {
            task_id: record.0.parse().map_err(StoreError::InvalidTaskId)?,
            repository_id: record.1.parse()?,
            attempt,
        },
        base_commit: record.3,
        branch_name: record.4,
        worktree_path: CanonicalPath::try_from_canonical(PathBuf::from(record.5))?,
        state: AttemptArtifactState::parse(&record.6)?,
        failure_code: record.7,
        created_at: UtcTimestamp::parse_rfc3339(&record.8)?,
        updated_at: UtcTimestamp::parse_rfc3339(&record.9)?,
    })
}

fn validate_reservation(input: &ReserveAttemptArtifact) -> Result<(), StoreError> {
    let valid_commit = matches!(input.base_commit.len(), 40 | 64)
        && input
            .base_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
    if input.identity.attempt == 0
        || !valid_commit
        || input.branch_name.is_empty()
        || input.worktree_path.to_string().is_empty()
    {
        return Err(StoreError::InvalidArtifactInput);
    }
    Ok(())
}

fn reservation_matches(artifact: &TaskAttemptArtifact, input: &ReserveAttemptArtifact) -> bool {
    artifact.identity == input.identity
        && artifact.base_commit == input.base_commit
        && artifact.branch_name == input.branch_name
        && artifact.worktree_path == input.worktree_path
}

fn current_timestamp() -> Result<UtcTimestamp, StoreError> {
    UtcTimestamp::new(OffsetDateTime::now_utc()).map_err(StoreError::Domain)
}
