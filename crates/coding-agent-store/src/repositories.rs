use std::path::PathBuf;

use coding_agent_domain::{CanonicalPath, NewRepository, Repository, RepositoryId, UtcTimestamp};
use time::OffsetDateTime;

use crate::{Store, StoreError};

type RepositoryRecord = (String, String, String, String, String, String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterRepositoryOutcome {
    Created(Repository),
    Existing(Repository),
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
