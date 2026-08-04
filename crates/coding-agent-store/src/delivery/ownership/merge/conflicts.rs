use std::collections::HashSet;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sqlx::{Row, SqliteConnection};

use crate::StoreError;
use crate::delivery::{DeliveryOperationId, MergeConflictPathEncoding, MergeConflictRecord};

use super::super::ownership_invariant;

pub(super) async fn load_merge_conflicts(
    connection: &mut SqliteConnection,
    operation_id: DeliveryOperationId,
    expected_count: Option<usize>,
) -> Result<Vec<MergeConflictRecord>, StoreError> {
    let rows = sqlx::query(
        "SELECT ordinal, CAST(path_encoding AS BLOB) AS path_encoding, \
                CAST(path_value AS BLOB) AS path_value \
         FROM task_merge_conflicts \
         WHERE operation_id = ? ORDER BY ordinal LIMIT 129",
    )
    .bind(operation_id.to_string())
    .fetch_all(connection)
    .await?;
    if rows.len() > 128 || rows.len() != expected_count.unwrap_or(0) {
        return Err(ownership_invariant());
    }
    let mut total_wire_bytes = 0usize;
    let mut decoded_paths = HashSet::with_capacity(rows.len());
    let mut conflicts = Vec::with_capacity(rows.len());
    for (expected_ordinal, row) in rows.into_iter().enumerate() {
        let ordinal_value: i64 = row.try_get("ordinal").map_err(|_| ownership_invariant())?;
        if ordinal_value != i64::try_from(expected_ordinal).map_err(|_| ownership_invariant())? {
            return Err(ownership_invariant());
        }
        let ordinal = u8::try_from(ordinal_value).map_err(|_| ownership_invariant())?;
        let encoding: Vec<u8> = row
            .try_get("path_encoding")
            .map_err(|_| ownership_invariant())?;
        let wire: Vec<u8> = row
            .try_get("path_value")
            .map_err(|_| ownership_invariant())?;
        if wire.is_empty() || wire.len() > 4096 {
            return Err(ownership_invariant());
        }
        total_wire_bytes = total_wire_bytes
            .checked_add(wire.len())
            .ok_or_else(ownership_invariant)?;
        if total_wire_bytes > 65_536 {
            return Err(ownership_invariant());
        }
        let (path_encoding, raw) = match encoding.as_slice() {
            b"utf8" => {
                std::str::from_utf8(&wire).map_err(|_| ownership_invariant())?;
                (MergeConflictPathEncoding::Utf8, wire.clone())
            }
            b"base64url" => {
                let encoded = std::str::from_utf8(&wire).map_err(|_| ownership_invariant())?;
                if encoded.contains('=') {
                    return Err(ownership_invariant());
                }
                let raw = URL_SAFE_NO_PAD
                    .decode(encoded)
                    .map_err(|_| ownership_invariant())?;
                if URL_SAFE_NO_PAD.encode(&raw).as_bytes() != wire
                    || std::str::from_utf8(&raw).is_ok()
                {
                    return Err(ownership_invariant());
                }
                (MergeConflictPathEncoding::Base64Url, raw)
            }
            _ => return Err(ownership_invariant()),
        };
        if !crate::delivery::merges::raw_relative_path_is_canonical(&raw)
            || !decoded_paths.insert(raw)
        {
            return Err(ownership_invariant());
        }
        conflicts.push(MergeConflictRecord {
            ordinal,
            path_encoding,
            path_value: wire,
        });
    }
    Ok(conflicts)
}
