use crate::StoreError;
use crate::delivery::{DeliveryOperationId, MergeConflictPathEncoding, MergeOperationRecord};

use super::merge_invariant;
use super::model::MergeConflictPaths;

pub(super) async fn insert_conflict_paths(
    connection: &mut sqlx::SqliteConnection,
    operation_id: DeliveryOperationId,
    paths: &MergeConflictPaths,
) -> Result<(), StoreError> {
    for path in &paths.encoded {
        let encoding = match path.path_encoding {
            MergeConflictPathEncoding::Utf8 => "utf8",
            MergeConflictPathEncoding::Base64Url => "base64url",
        };
        let value = std::str::from_utf8(&path.path_value).map_err(|_| merge_invariant())?;
        let inserted = sqlx::query(
            "INSERT INTO task_merge_conflicts \
             (operation_id, ordinal, path_encoding, path_value) VALUES (?, ?, ?, ?)",
        )
        .bind(operation_id.to_string())
        .bind(i64::from(path.ordinal))
        .bind(encoding)
        .bind(value)
        .execute(&mut *connection)
        .await?;
        if inserted.rows_affected() != 1 {
            return Err(merge_invariant());
        }
    }
    Ok(())
}

pub(super) fn conflict_paths_match(
    operation: &MergeOperationRecord,
    paths: &MergeConflictPaths,
) -> bool {
    operation.conflict_path_count == u8::try_from(paths.len()).ok()
        && operation.conflicts == paths.encoded
}
