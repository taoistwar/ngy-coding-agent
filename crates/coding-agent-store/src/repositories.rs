use std::fmt;
use std::path::PathBuf;

use coding_agent_domain::{CanonicalPath, NewRepository, Repository, RepositoryId, UtcTimestamp};
use time::OffsetDateTime;

use crate::{Store, StoreError};

type RepositoryRecord = (String, String, String, String, String, String, String);
type RepositoryIdentityLookupRecord = (String, String, String);

const REPOSITORY_IDENTITY_LOOKUP_INVARIANT: &str =
    "repository identity lookup projection is inconsistent";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterRepositoryOutcome {
    Created(Repository),
    Existing(Repository),
}

#[derive(Clone, PartialEq, Eq)]
pub struct RepositoryIdentityLookup {
    pub repository_id: RepositoryId,
    pub git_root: CanonicalPath,
    pub git_identity_key: String,
}

impl fmt::Debug for RepositoryIdentityLookup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepositoryIdentityLookup")
            .field("repository_id", &self.repository_id)
            .field("git_root", &"<redacted>")
            .field("git_identity_key", &"<redacted>")
            .finish()
    }
}

impl Store {
    pub async fn register_repository(
        &self,
        input: NewRepository,
    ) -> Result<RegisterRepositoryOutcome, StoreError> {
        let git_identity_key = identity_key(&input.git_root);
        let cargo_identity_key = identity_key(&input.cargo_workspace_root);
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let now = UtcTimestamp::new(OffsetDateTime::now_utc())?;
        let now_text = now.to_string();

        let existing: Option<RepositoryRecord> = sqlx::query_as(
            "SELECT id, selected_path, display_name, git_root, cargo_workspace_root, \
                    created_at, last_opened_at \
             FROM repositories \
             WHERE git_identity_key = ? AND cargo_identity_key = ?",
        )
        .bind(&git_identity_key)
        .bind(&cargo_identity_key)
        .fetch_optional(&mut *transaction)
        .await?;

        if let Some(mut record) = existing {
            let selected_path = input.selected_path.to_string();
            sqlx::query(
                "UPDATE repositories \
                 SET selected_path = ?, last_opened_at = ? \
                 WHERE id = ?",
            )
            .bind(&selected_path)
            .bind(&now_text)
            .bind(&record.0)
            .execute(&mut *transaction)
            .await?;

            record.1 = selected_path;
            record.6 = now_text;
            let repository = repository_from_record(record)?;
            transaction.commit().await?;
            return Ok(RegisterRepositoryOutcome::Existing(repository));
        }

        let id = RepositoryId::new();
        sqlx::query(
            "INSERT INTO repositories (\
                 id, selected_path, display_name, git_root, cargo_workspace_root,\
                 git_identity_key, cargo_identity_key, created_at, last_opened_at\
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.selected_path.to_string())
        .bind(&input.display_name)
        .bind(input.git_root.to_string())
        .bind(input.cargo_workspace_root.to_string())
        .bind(git_identity_key)
        .bind(cargo_identity_key)
        .bind(&now_text)
        .bind(&now_text)
        .execute(&mut *transaction)
        .await?;

        let repository = Repository {
            id,
            selected_path: input.selected_path,
            display_name: input.display_name,
            git_root: input.git_root,
            cargo_workspace_root: input.cargo_workspace_root,
            created_at: now,
            last_opened_at: now,
        };
        transaction.commit().await?;
        Ok(RegisterRepositoryOutcome::Created(repository))
    }

    pub async fn list_repositories(&self) -> Result<Vec<Repository>, StoreError> {
        let records: Vec<RepositoryRecord> = sqlx::query_as(
            "SELECT id, selected_path, display_name, git_root, cargo_workspace_root, \
                    created_at, last_opened_at \
             FROM repositories \
             ORDER BY last_opened_at DESC, id",
        )
        .fetch_all(&self.pool)
        .await?;

        records.into_iter().map(repository_from_record).collect()
    }

    pub async fn list_repository_identity_lookups(
        &self,
    ) -> Result<Vec<RepositoryIdentityLookup>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let invalid_storage_classes: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM repositories \
             WHERE typeof(id) != 'text' \
                OR typeof(git_root) != 'text' \
                OR typeof(git_identity_key) != 'text'",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if invalid_storage_classes != 0 {
            return Err(repository_identity_lookup_invariant());
        }

        let records: Vec<RepositoryIdentityLookupRecord> = sqlx::query_as(
            "SELECT id, git_root, git_identity_key \
             FROM repositories \
             ORDER BY id ASC",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let lookups = records
            .into_iter()
            .map(repository_identity_lookup_from_record)
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await?;
        Ok(lookups)
    }

    /// Loads the exact durable identity projection for one repository.
    ///
    /// Runtime registration deliberately uses the row selected by its opaque
    /// repository ID. It never attempts to rediscover or guess an identity
    /// from a request path after an ambiguous writer outcome.
    pub async fn repository_identity_lookup(
        &self,
        repository_id: RepositoryId,
    ) -> Result<Option<RepositoryIdentityLookup>, StoreError> {
        let record: Option<RepositoryIdentityLookupRecord> = sqlx::query_as(
            "SELECT id, git_root, git_identity_key \
             FROM repositories \
             WHERE id = ?",
        )
        .bind(repository_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        record
            .map(repository_identity_lookup_from_record)
            .transpose()
    }
}

fn repository_from_record(record: RepositoryRecord) -> Result<Repository, StoreError> {
    Ok(Repository {
        id: record.0.parse()?,
        selected_path: CanonicalPath::try_from_canonical(PathBuf::from(record.1))?,
        display_name: record.2,
        git_root: CanonicalPath::try_from_canonical(PathBuf::from(record.3))?,
        cargo_workspace_root: CanonicalPath::try_from_canonical(PathBuf::from(record.4))?,
        created_at: UtcTimestamp::parse_rfc3339(&record.5)?,
        last_opened_at: UtcTimestamp::parse_rfc3339(&record.6)?,
    })
}

fn repository_identity_lookup_from_record(
    record: RepositoryIdentityLookupRecord,
) -> Result<RepositoryIdentityLookup, StoreError> {
    let repository_id = record
        .0
        .parse::<RepositoryId>()
        .map_err(|_| repository_identity_lookup_invariant())?;
    let git_root = CanonicalPath::try_from_canonical(PathBuf::from(&record.1))
        .map_err(|_| repository_identity_lookup_invariant())?;
    if record.0 != repository_id.to_string()
        || record.1 != git_root.to_string()
        || record.2 != identity_key(&git_root)
    {
        return Err(repository_identity_lookup_invariant());
    }

    Ok(RepositoryIdentityLookup {
        repository_id,
        git_root,
        git_identity_key: record.2,
    })
}

fn repository_identity_lookup_invariant() -> StoreError {
    StoreError::InvariantViolation(REPOSITORY_IDENTITY_LOOKUP_INVARIANT)
}

fn identity_key(path: &CanonicalPath) -> String {
    #[cfg(windows)]
    {
        windows_identity_key(&path.to_string())
    }

    #[cfg(not(windows))]
    {
        path.to_string()
    }
}

#[cfg(windows)]
fn windows_identity_key(path: &str) -> String {
    path.replace('/', "\\").to_lowercase()
}
