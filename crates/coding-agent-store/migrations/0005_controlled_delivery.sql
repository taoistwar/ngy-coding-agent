CREATE TABLE task_delivery_operation_transitions (
    transition_id INTEGER PRIMARY KEY,
    entity_kind TEXT NOT NULL
        CHECK (
            typeof(entity_kind) = 'text'
            AND entity_kind IN (
                'delivery_source',
                'merge_operation',
                'cleanup_operation',
                'worktree_disposition',
                'branch_disposition'
            )
        ),
    entity_id TEXT NOT NULL
        CHECK (
            typeof(entity_id) = 'text'
            AND length(CAST(entity_id AS BLOB)) = 36
            AND substr(entity_id, 9, 1) = '-'
            AND substr(entity_id, 14, 1) = '-'
            AND substr(entity_id, 19, 1) = '-'
            AND substr(entity_id, 24, 1) = '-'
            AND length(CAST(replace(entity_id, '-', '') AS BLOB)) = 32
            AND replace(entity_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND entity_id != '00000000-0000-0000-0000-000000000000'
        ),
    entity_version INTEGER NOT NULL
        CHECK (
            typeof(entity_version) = 'integer'
            AND entity_version BETWEEN 1 AND 9007199254740991
        ),
    from_state TEXT NOT NULL CHECK (typeof(from_state) = 'text'),
    to_state TEXT NOT NULL CHECK (typeof(to_state) = 'text'),
    failure_code TEXT
        CHECK (
            failure_code IS NULL
            OR (
                typeof(failure_code) = 'text'
                AND length(CAST(failure_code AS BLOB)) BETWEEN 1 AND 128
                AND substr(failure_code, 1, 1) GLOB '[A-Z]'
                AND substr(failure_code, -1, 1) GLOB '[A-Z0-9]'
                AND failure_code NOT GLOB '*[^A-Z0-9_]*'
            )
        ),
    target_config_attributes_digest TEXT
        CHECK (
            target_config_attributes_digest IS NULL
            OR (
                typeof(target_config_attributes_digest) = 'text'
                AND length(CAST(target_config_attributes_digest AS BLOB)) = 64
                AND target_config_attributes_digest NOT GLOB '*[^0-9a-f]*'
            )
        ),
    target_security_digest TEXT
        CHECK (
            target_security_digest IS NULL
            OR (
                typeof(target_security_digest) = 'text'
                AND length(CAST(target_security_digest AS BLOB)) = 64
                AND target_security_digest NOT GLOB '*[^0-9a-f]*'
            )
        ),
    transitioned_at TEXT NOT NULL
        CHECK (
            typeof(transitioned_at) = 'text'
            AND length(CAST(transitioned_at AS BLOB)) = 30
            AND transitioned_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(transitioned_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(transitioned_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(transitioned_at, 1, 19), '+0 seconds')
                = substr(transitioned_at, 1, 19)
        ),
    UNIQUE (entity_kind, entity_id, entity_version),
    UNIQUE (entity_kind, entity_id, entity_version, to_state),
    CHECK (
        (entity_version = 1 AND from_state = 'absent')
        OR (entity_version > 1 AND from_state != 'absent')
    ),
    CHECK (
        (
            entity_kind = 'merge_operation'
            AND target_config_attributes_digest IS NOT NULL
            AND target_security_digest IS NOT NULL
        )
        OR (
            entity_kind != 'merge_operation'
            AND target_config_attributes_digest IS NULL
            AND target_security_digest IS NULL
        )
    ),
    CHECK (
        (
            entity_kind = 'delivery_source'
            AND (
                (
                    from_state = 'absent'
                    AND to_state = 'object_pending'
                    AND failure_code IS NULL
                )
                OR (
                    from_state = 'object_pending'
                    AND to_state = 'object_pending'
                    AND failure_code IS 'COMMAND_TIMED_OUT'
                )
                OR (
                    from_state = 'object_pending'
                    AND to_state = 'commit_pending'
                    AND failure_code IS NULL
                )
                OR (
                    from_state = 'commit_pending'
                    AND to_state = 'commit_pending'
                    AND failure_code IS 'COMMAND_TIMED_OUT'
                )
                OR (
                    from_state = 'commit_pending'
                    AND to_state = 'committed'
                    AND failure_code IS NULL
                )
                OR (
                    from_state IN ('object_pending', 'commit_pending', 'committed')
                    AND to_state = 'reconciliation_required'
                    AND failure_code IS NOT NULL
                    AND failure_code IN (
                        'DELIVERY_SOURCE_INCONSISTENT',
                        'PROCESS_TREE_CLEANUP_FAILED'
                    )
                )
            )
        )
        OR (
            entity_kind = 'merge_operation'
            AND (
                (to_state IN (
                    'preflight_pending', 'preflight_ready', 'accepted',
                    'merge_pending', 'merged', 'abort_pending', 'superseded'
                ) AND failure_code IS NULL)
                OR (to_state = 'conflict' AND failure_code IS NOT NULL
                    AND failure_code = 'MERGE_CONFLICT')
                OR (to_state = 'rejected' AND failure_code IS NOT NULL AND failure_code IN (
                    'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED',
                    'TARGET_BRANCH_MISMATCH', 'TARGET_WORKTREE_DIRTY',
                    'TARGET_IGNORED_PATH_COLLISION', 'TARGET_GIT_OPERATION_IN_PROGRESS',
                    'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES',
                    'SOURCE_ALREADY_IN_TARGET'
                ))
                OR (to_state = 'stale' AND failure_code IS NOT NULL AND failure_code IN (
                    'DELIVERY_EVIDENCE_STALE', 'TARGET_BRANCH_MISMATCH',
                    'TARGET_HEAD_CHANGED', 'DELIVERY_SOURCE_CHANGED'
                ))
                OR (to_state = 'failed' AND failure_code IS NOT NULL AND failure_code IN (
                    'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED',
                    'TARGET_BRANCH_MISMATCH', 'TARGET_WORKTREE_DIRTY',
                    'TARGET_IGNORED_PATH_COLLISION', 'TARGET_GIT_OPERATION_IN_PROGRESS',
                    'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES',
                    'SOURCE_ALREADY_IN_TARGET', 'TARGET_HEAD_CHANGED', 'COMMAND_TIMED_OUT'
                ))
                OR (to_state = 'reconciliation_required' AND failure_code IS NOT NULL
                    AND failure_code IN (
                    'DELIVERY_RECONCILIATION_REQUIRED', 'DELIVERY_SOURCE_INCONSISTENT',
                    'PROCESS_TREE_CLEANUP_FAILED', 'WORKTREE_IDENTITY_MISMATCH',
                    'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES'
                ))
            )
        )
        OR (
            entity_kind = 'cleanup_operation'
            AND (
                (to_state = 'completed' AND failure_code IS NULL)
                OR (to_state IN ('failed', 'reconciliation_required') AND failure_code IS NOT NULL)
                OR to_state IN (
                    'unlock_pending', 'unlocked_pending_remove',
                    'remove_pending', 'delete_pending'
                )
            )
        )
        OR (
            entity_kind IN ('worktree_disposition', 'branch_disposition')
            AND (
                (to_state = 'reconciliation_required' AND failure_code IS NOT NULL)
                OR (to_state != 'reconciliation_required' AND failure_code IS NULL)
            )
        )
    ),
    CHECK (
        (
            entity_kind = 'delivery_source'
            AND from_state IN (
                'absent', 'object_pending', 'commit_pending', 'committed'
            )
            AND to_state IN (
                'object_pending', 'commit_pending', 'committed',
                'reconciliation_required'
            )
            AND (
                (from_state = 'absent' AND to_state = 'object_pending')
                OR (from_state = 'object_pending' AND to_state IN (
                    'object_pending', 'commit_pending', 'reconciliation_required'
                ))
                OR (from_state = 'commit_pending' AND to_state IN (
                    'commit_pending', 'committed', 'reconciliation_required'
                ))
                OR (from_state = 'committed' AND to_state = 'reconciliation_required')
            )
        )
        OR (
            entity_kind = 'merge_operation'
            AND from_state IN (
                'absent', 'preflight_pending', 'preflight_ready', 'accepted',
                'merge_pending', 'abort_pending'
            )
            AND to_state IN (
                'preflight_pending', 'preflight_ready', 'accepted',
                'merge_pending', 'merged', 'abort_pending', 'conflict',
                'rejected', 'stale', 'superseded', 'failed',
                'reconciliation_required'
            )
            AND (
                (from_state = 'absent' AND to_state = 'preflight_pending'
                    AND entity_version = 1)
                OR (from_state = 'preflight_pending'
                    AND to_state = 'preflight_pending' AND entity_version = 2)
                OR (from_state = 'preflight_pending' AND (
                    (to_state IN ('preflight_ready', 'conflict') AND entity_version = 3)
                    OR (to_state IN ('rejected', 'stale', 'reconciliation_required')
                        AND entity_version IN (2, 3))
                ))
                OR (from_state = 'preflight_ready' AND to_state IN (
                    'accepted', 'stale', 'superseded', 'reconciliation_required'
                ))
                OR (from_state = 'accepted' AND to_state IN (
                    'merge_pending', 'failed', 'reconciliation_required'
                ))
                OR (from_state = 'merge_pending' AND to_state IN (
                    'merged', 'abort_pending', 'failed',
                    'reconciliation_required'
                ))
                OR (from_state = 'abort_pending' AND to_state IN (
                    'conflict', 'reconciliation_required'
                ))
            )
        )
        OR (
            entity_kind = 'cleanup_operation'
            AND from_state IN (
                'absent', 'unlock_pending', 'unlocked_pending_remove',
                'remove_pending', 'delete_pending'
            )
            AND to_state IN (
                'unlock_pending', 'unlocked_pending_remove', 'remove_pending',
                'delete_pending', 'completed', 'failed',
                'reconciliation_required'
            )
            AND (
                (from_state = 'absent' AND to_state IN (
                    'unlock_pending', 'remove_pending', 'delete_pending'
                ))
                OR (from_state = 'unlock_pending' AND to_state IN (
                    'unlocked_pending_remove', 'failed',
                    'reconciliation_required'
                ))
                OR (from_state = 'unlocked_pending_remove' AND to_state IN (
                    'remove_pending', 'reconciliation_required'
                ))
                OR (from_state = 'remove_pending' AND to_state IN (
                    'completed', 'failed', 'reconciliation_required'
                ))
                OR (from_state = 'delete_pending' AND to_state IN (
                    'delete_pending', 'completed', 'failed',
                    'reconciliation_required'
                ))
            )
        )
        OR (
            entity_kind = 'worktree_disposition'
            AND from_state IN (
                'absent', 'retained_locked', 'retained_unlocked', 'removed'
            )
            AND to_state IN (
                'retained_locked', 'retained_unlocked', 'removed',
                'reconciliation_required'
            )
            AND (
                (from_state = 'absent' AND to_state = 'retained_locked')
                OR (from_state = 'retained_locked' AND to_state IN (
                    'retained_unlocked', 'reconciliation_required'
                ))
                OR (from_state = 'retained_unlocked' AND to_state IN (
                    'removed', 'reconciliation_required'
                ))
                OR (from_state = 'removed' AND to_state = 'reconciliation_required')
            )
        )
        OR (
            entity_kind = 'branch_disposition'
            AND from_state IN ('absent', 'retained', 'deleted')
            AND to_state IN ('retained', 'deleted', 'reconciliation_required')
            AND (
                (from_state = 'absent' AND to_state = 'retained')
                OR (from_state = 'retained' AND to_state IN (
                    'deleted', 'reconciliation_required'
                ))
                OR (from_state = 'deleted' AND to_state = 'reconciliation_required')
            )
        )
    ),
    CHECK (
        instr(CAST(entity_kind AS BLOB), x'00') = 0
        AND instr(CAST(entity_id AS BLOB), x'00') = 0
        AND instr(CAST(from_state AS BLOB), x'00') = 0
        AND instr(CAST(to_state AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(failure_code, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(target_config_attributes_digest, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(target_security_digest, '') AS BLOB), x'00') = 0
        AND instr(CAST(transitioned_at AS BLOB), x'00') = 0
    )
) STRICT;

CREATE TABLE task_delivery_sources (
    task_id TEXT PRIMARY KEY NOT NULL
        CHECK (
            typeof(task_id) = 'text'
            AND length(CAST(task_id AS BLOB)) = 36
            AND substr(task_id, 9, 1) = '-'
            AND substr(task_id, 14, 1) = '-'
            AND substr(task_id, 19, 1) = '-'
            AND substr(task_id, 24, 1) = '-'
            AND length(CAST(replace(task_id, '-', '') AS BLOB)) = 32
            AND replace(task_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND task_id != '00000000-0000-0000-0000-000000000000'
        ),
    repository_id TEXT NOT NULL
        CHECK (
            typeof(repository_id) = 'text'
            AND length(CAST(repository_id AS BLOB)) = 36
            AND substr(repository_id, 9, 1) = '-'
            AND substr(repository_id, 14, 1) = '-'
            AND substr(repository_id, 19, 1) = '-'
            AND substr(repository_id, 24, 1) = '-'
            AND length(CAST(replace(repository_id, '-', '') AS BLOB)) = 32
            AND replace(repository_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND repository_id != '00000000-0000-0000-0000-000000000000'
        ),
    attempt INTEGER NOT NULL
        CHECK (typeof(attempt) = 'integer' AND attempt BETWEEN 1 AND 4294967295),
    evidence_algorithm TEXT NOT NULL
        CHECK (typeof(evidence_algorithm) = 'text' AND evidence_algorithm = 'evidence_identity_v1'),
    final_review_round INTEGER NOT NULL
        CHECK (typeof(final_review_round) = 'integer' AND final_review_round BETWEEN 1 AND 3),
    final_review_event_id INTEGER NOT NULL
        CHECK (typeof(final_review_event_id) = 'integer' AND final_review_event_id > 0),
    workspace_generation INTEGER NOT NULL
        CHECK (
            typeof(workspace_generation) = 'integer'
            AND workspace_generation BETWEEN 0 AND 9007199254740991
        ),
    workspace_fingerprint TEXT NOT NULL
        CHECK (
            typeof(workspace_fingerprint) = 'text'
            AND length(CAST(workspace_fingerprint AS BLOB)) = 64
            AND workspace_fingerprint NOT GLOB '*[^0-9a-f]*'
        ),
    checks_digest TEXT NOT NULL
        CHECK (
            typeof(checks_digest) = 'text'
            AND length(CAST(checks_digest AS BLOB)) = 64
            AND checks_digest NOT GLOB '*[^0-9a-f]*'
        ),
    coverage_digest TEXT NOT NULL
        CHECK (
            typeof(coverage_digest) = 'text'
            AND length(CAST(coverage_digest AS BLOB)) = 64
            AND coverage_digest NOT GLOB '*[^0-9a-f]*'
        ),
    artifact_base_commit TEXT NOT NULL
        CHECK (
            typeof(artifact_base_commit) = 'text'
            AND length(CAST(artifact_base_commit AS BLOB)) IN (40, 64)
            AND artifact_base_commit NOT GLOB '*[^0-9a-f]*'
        ),
    artifact_source_branch TEXT NOT NULL
        CHECK (
            typeof(artifact_source_branch) = 'text'
            AND substr(artifact_source_branch, 1, 11) = 'refs/heads/'
            AND length(CAST(artifact_source_branch AS BLOB)) BETWEEN 12 AND 4096
            AND substr(artifact_source_branch, 12, 1) != '-'
            AND substr(artifact_source_branch, -1, 1) NOT IN ('/', '.')
            AND instr(artifact_source_branch, '..') = 0
            AND instr(artifact_source_branch, '@{') = 0
            AND instr(artifact_source_branch, '//') = 0
            AND instr(artifact_source_branch, ' ') = 0
            AND instr(artifact_source_branch, '~') = 0
            AND instr(artifact_source_branch, '^') = 0
            AND instr(artifact_source_branch, ':') = 0
            AND instr(artifact_source_branch, '?') = 0
            AND instr(artifact_source_branch, '*') = 0
            AND instr(artifact_source_branch, '[') = 0
            AND instr(artifact_source_branch, '\') = 0
            AND instr(artifact_source_branch, char(0)) = 0
            AND substr(artifact_source_branch, 12) NOT GLOB '.*'
            AND instr(substr(artifact_source_branch, 12), '/.') = 0
            AND substr(artifact_source_branch, -5) != '.lock'
            AND instr(substr(artifact_source_branch, 12), '.lock/') = 0
        ),
    artifact_worktree_path TEXT NOT NULL
        CHECK (
            typeof(artifact_worktree_path) = 'text'
            AND length(CAST(artifact_worktree_path AS BLOB)) BETWEEN 1 AND 32768
        ),
    common_git_identity_algorithm TEXT NOT NULL
        CHECK (
            typeof(common_git_identity_algorithm) = 'text'
            AND common_git_identity_algorithm = 'directory_identity_v1'
        ),
    common_git_identity_digest TEXT NOT NULL
        CHECK (
            typeof(common_git_identity_digest) = 'text'
            AND length(CAST(common_git_identity_digest AS BLOB)) = 64
            AND common_git_identity_digest NOT GLOB '*[^0-9a-f]*'
        ),
    worktree_admin_identity_algorithm TEXT NOT NULL
        CHECK (
            typeof(worktree_admin_identity_algorithm) = 'text'
            AND worktree_admin_identity_algorithm = 'directory_identity_v1'
        ),
    worktree_admin_identity_digest TEXT NOT NULL
        CHECK (
            typeof(worktree_admin_identity_digest) = 'text'
            AND length(CAST(worktree_admin_identity_digest AS BLOB)) = 64
            AND worktree_admin_identity_digest NOT GLOB '*[^0-9a-f]*'
        ),
    fixed_lock_reason TEXT NOT NULL
        CHECK (typeof(fixed_lock_reason) = 'text' AND fixed_lock_reason = 'codex-reserved'),
    config_attributes_digest TEXT NOT NULL
        CHECK (
            typeof(config_attributes_digest) = 'text'
            AND length(CAST(config_attributes_digest AS BLOB)) = 64
            AND config_attributes_digest NOT GLOB '*[^0-9a-f]*'
        ),
    origin_accepted_operation_id TEXT NOT NULL
        CHECK (
            typeof(origin_accepted_operation_id) = 'text'
            AND length(CAST(origin_accepted_operation_id AS BLOB)) = 36
            AND substr(origin_accepted_operation_id, 9, 1) = '-'
            AND substr(origin_accepted_operation_id, 14, 1) = '-'
            AND substr(origin_accepted_operation_id, 19, 1) = '-'
            AND substr(origin_accepted_operation_id, 24, 1) = '-'
            AND length(CAST(replace(origin_accepted_operation_id, '-', '') AS BLOB)) = 32
            AND replace(origin_accepted_operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND origin_accepted_operation_id != '00000000-0000-0000-0000-000000000000'
        ),
    origin_accept_receipt_id TEXT NOT NULL
        CHECK (
            typeof(origin_accept_receipt_id) = 'text'
            AND length(CAST(origin_accept_receipt_id AS BLOB)) = 36
            AND substr(origin_accept_receipt_id, 9, 1) = '-'
            AND substr(origin_accept_receipt_id, 14, 1) = '-'
            AND substr(origin_accept_receipt_id, 19, 1) = '-'
            AND substr(origin_accept_receipt_id, 24, 1) = '-'
            AND length(CAST(replace(origin_accept_receipt_id, '-', '') AS BLOB)) = 32
            AND replace(origin_accept_receipt_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND origin_accept_receipt_id != '00000000-0000-0000-0000-000000000000'
        ),
    origin_accepted_version INTEGER NOT NULL
        CHECK (
            typeof(origin_accepted_version) = 'integer'
            AND origin_accepted_version BETWEEN 1 AND 9007199254740991
        ),
    candidate_tree_oid TEXT NOT NULL
        CHECK (
            typeof(candidate_tree_oid) = 'text'
            AND length(CAST(candidate_tree_oid AS BLOB)) IN (40, 64)
            AND candidate_tree_oid NOT GLOB '*[^0-9a-f]*'
        ),
    expected_parent_oid TEXT NOT NULL
        CHECK (
            typeof(expected_parent_oid) = 'text'
            AND length(CAST(expected_parent_oid AS BLOB)) IN (40, 64)
            AND expected_parent_oid NOT GLOB '*[^0-9a-f]*'
        ),
    expected_source_commit_oid TEXT
        CHECK (
            expected_source_commit_oid IS NULL
            OR (
                typeof(expected_source_commit_oid) = 'text'
                AND length(CAST(expected_source_commit_oid AS BLOB)) IN (40, 64)
                AND expected_source_commit_oid NOT GLOB '*[^0-9a-f]*'
            )
        ),
    author_name TEXT NOT NULL CHECK (typeof(author_name) = 'text' AND author_name = 'Coding Agent'),
    author_email TEXT NOT NULL
        CHECK (typeof(author_email) = 'text' AND author_email = 'coding-agent@localhost'),
    committer_name TEXT NOT NULL
        CHECK (typeof(committer_name) = 'text' AND committer_name = 'Coding Agent'),
    committer_email TEXT NOT NULL
        CHECK (typeof(committer_email) = 'text' AND committer_email = 'coding-agent@localhost'),
    author_date_bytes TEXT NOT NULL
        CHECK (
            typeof(author_date_bytes) = 'text'
            AND length(CAST(author_date_bytes AS BLOB)) BETWEEN 7 AND 64
            AND substr(author_date_bytes, -6) = ' +0000'
            AND CAST(
                CAST(substr(author_date_bytes, 1, length(author_date_bytes) - 6) AS INTEGER)
                AS TEXT
            ) = substr(author_date_bytes, 1, length(author_date_bytes) - 6)
        ),
    committer_date_bytes TEXT NOT NULL
        CHECK (
            typeof(committer_date_bytes) = 'text'
            AND length(CAST(committer_date_bytes AS BLOB)) BETWEEN 7 AND 64
            AND substr(committer_date_bytes, -6) = ' +0000'
            AND CAST(
                CAST(substr(committer_date_bytes, 1, length(committer_date_bytes) - 6) AS INTEGER)
                AS TEXT
            ) = substr(committer_date_bytes, 1, length(committer_date_bytes) - 6)
        ),
    commit_message_template_version INTEGER NOT NULL
        CHECK (typeof(commit_message_template_version) = 'integer' AND commit_message_template_version = 1),
    commit_message_bytes BLOB NOT NULL
        CHECK (
            typeof(commit_message_bytes) = 'blob'
            AND length(commit_message_bytes) BETWEEN 1 AND 512
        ),
    state TEXT NOT NULL
        CHECK (
            typeof(state) = 'text'
            AND state IN (
                'object_pending', 'commit_pending', 'committed',
                'reconciliation_required'
            )
        ),
    required_merge_reconciliation_key TEXT
        GENERATED ALWAYS AS (
            CASE
                WHEN state = 'reconciliation_required' THEN 'task:' || task_id
                ELSE NULL
            END
        ) STORED,
    failure_code TEXT
        CHECK (
            failure_code IS NULL
            OR (
                typeof(failure_code) = 'text'
                AND length(CAST(failure_code AS BLOB)) BETWEEN 1 AND 128
                AND substr(failure_code, 1, 1) GLOB '[A-Z]'
                AND substr(failure_code, -1, 1) GLOB '[A-Z0-9]'
                AND failure_code NOT GLOB '*[^A-Z0-9_]*'
            )
        ),
    version INTEGER NOT NULL
        CHECK (typeof(version) = 'integer' AND version BETWEEN 1 AND 9007199254740991),
    created_at TEXT NOT NULL
        CHECK (
            typeof(created_at) = 'text'
            AND length(CAST(created_at AS BLOB)) = 30
            AND created_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(created_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19), '+0 seconds')
                = substr(created_at, 1, 19)
        ),
    updated_at TEXT NOT NULL
        CHECK (
            typeof(updated_at) = 'text'
            AND length(CAST(updated_at AS BLOB)) = 30
            AND updated_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(updated_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(updated_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(updated_at, 1, 19), '+0 seconds')
                = substr(updated_at, 1, 19)
        ),
    UNIQUE (task_id, expected_source_commit_oid),
    FOREIGN KEY (task_id, repository_id, attempt)
        REFERENCES tasks (id, repository_id, attempt),
    FOREIGN KEY (repository_id, task_id, attempt)
        REFERENCES task_attempt_artifacts (repository_id, task_id, attempt),
    FOREIGN KEY (final_review_event_id)
        REFERENCES task_review_evidence (event_id),
    FOREIGN KEY (origin_accepted_operation_id)
        REFERENCES task_merge_operations (operation_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (origin_accept_receipt_id)
        REFERENCES task_delivery_command_receipts (client_request_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (required_merge_reconciliation_key)
        REFERENCES task_merge_operations (merge_reconciliation_key)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (expected_parent_oid = artifact_base_commit),
    CHECK (
        length(CAST(candidate_tree_oid AS BLOB)) = length(CAST(artifact_base_commit AS BLOB))
        AND length(CAST(expected_parent_oid AS BLOB)) = length(CAST(artifact_base_commit AS BLOB))
        AND (
            expected_source_commit_oid IS NULL
            OR length(CAST(expected_source_commit_oid AS BLOB))
                = length(CAST(artifact_base_commit AS BLOB))
        )
    ),
    CHECK (author_date_bytes = committer_date_bytes),
    CHECK (
        (
            state IN ('object_pending', 'commit_pending')
            AND (failure_code IS NULL OR failure_code = 'COMMAND_TIMED_OUT')
        )
        OR (state = 'committed' AND failure_code IS NULL)
        OR (
            state = 'reconciliation_required'
            AND failure_code IS NOT NULL
            AND failure_code IN (
                'DELIVERY_SOURCE_INCONSISTENT',
                'PROCESS_TREE_CLEANUP_FAILED'
            )
        )
    ),
    CHECK (
        (state = 'object_pending' AND expected_source_commit_oid IS NULL)
        OR (
            state IN ('commit_pending', 'committed')
            AND expected_source_commit_oid IS NOT NULL
        )
        OR state = 'reconciliation_required'
    ),
    CHECK (
        artifact_base_commit GLOB '*[1-9a-f]*'
        AND candidate_tree_oid GLOB '*[1-9a-f]*'
        AND expected_parent_oid GLOB '*[1-9a-f]*'
        AND (
            expected_source_commit_oid IS NULL
            OR expected_source_commit_oid GLOB '*[1-9a-f]*'
        )
    ),
    CHECK (
        instr(CAST(task_id AS BLOB), x'00') = 0
        AND instr(CAST(repository_id AS BLOB), x'00') = 0
        AND instr(CAST(evidence_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(workspace_fingerprint AS BLOB), x'00') = 0
        AND instr(CAST(checks_digest AS BLOB), x'00') = 0
        AND instr(CAST(coverage_digest AS BLOB), x'00') = 0
        AND instr(CAST(artifact_base_commit AS BLOB), x'00') = 0
        AND instr(CAST(artifact_source_branch AS BLOB), x'00') = 0
        AND instr(CAST(artifact_worktree_path AS BLOB), x'00') = 0
        AND instr(CAST(common_git_identity_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(common_git_identity_digest AS BLOB), x'00') = 0
        AND instr(CAST(worktree_admin_identity_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(worktree_admin_identity_digest AS BLOB), x'00') = 0
        AND instr(CAST(fixed_lock_reason AS BLOB), x'00') = 0
        AND instr(CAST(config_attributes_digest AS BLOB), x'00') = 0
        AND instr(CAST(origin_accepted_operation_id AS BLOB), x'00') = 0
        AND instr(CAST(origin_accept_receipt_id AS BLOB), x'00') = 0
        AND instr(CAST(candidate_tree_oid AS BLOB), x'00') = 0
        AND instr(CAST(expected_parent_oid AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(expected_source_commit_oid, '') AS BLOB), x'00') = 0
        AND instr(CAST(author_name AS BLOB), x'00') = 0
        AND instr(CAST(author_email AS BLOB), x'00') = 0
        AND instr(CAST(committer_name AS BLOB), x'00') = 0
        AND instr(CAST(committer_email AS BLOB), x'00') = 0
        AND instr(CAST(author_date_bytes AS BLOB), x'00') = 0
        AND instr(CAST(committer_date_bytes AS BLOB), x'00') = 0
        AND instr(CAST(state AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(required_merge_reconciliation_key, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(failure_code, '') AS BLOB), x'00') = 0
        AND instr(CAST(created_at AS BLOB), x'00') = 0
        AND instr(CAST(updated_at AS BLOB), x'00') = 0
    )
) STRICT;

CREATE TABLE task_merge_operations (
    operation_id TEXT PRIMARY KEY NOT NULL
        CHECK (
            typeof(operation_id) = 'text'
            AND length(CAST(operation_id AS BLOB)) = 36
            AND substr(operation_id, 9, 1) = '-'
            AND substr(operation_id, 14, 1) = '-'
            AND substr(operation_id, 19, 1) = '-'
            AND substr(operation_id, 24, 1) = '-'
            AND length(CAST(replace(operation_id, '-', '') AS BLOB)) = 32
            AND replace(operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND operation_id != '00000000-0000-0000-0000-000000000000'
        ),
    task_id TEXT NOT NULL
        CHECK (
            typeof(task_id) = 'text'
            AND length(CAST(task_id AS BLOB)) = 36
            AND substr(task_id, 9, 1) = '-'
            AND substr(task_id, 14, 1) = '-'
            AND substr(task_id, 19, 1) = '-'
            AND substr(task_id, 24, 1) = '-'
            AND length(CAST(replace(task_id, '-', '') AS BLOB)) = 32
            AND replace(task_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND task_id != '00000000-0000-0000-0000-000000000000'
        ),
    repository_id TEXT NOT NULL
        CHECK (
            typeof(repository_id) = 'text'
            AND length(CAST(repository_id AS BLOB)) = 36
            AND substr(repository_id, 9, 1) = '-'
            AND substr(repository_id, 14, 1) = '-'
            AND substr(repository_id, 19, 1) = '-'
            AND substr(repository_id, 24, 1) = '-'
            AND length(CAST(replace(repository_id, '-', '') AS BLOB)) = 32
            AND replace(repository_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND repository_id != '00000000-0000-0000-0000-000000000000'
        ),
    attempt INTEGER NOT NULL
        CHECK (typeof(attempt) = 'integer' AND attempt BETWEEN 1 AND 4294967295),
    evidence_algorithm TEXT NOT NULL
        CHECK (typeof(evidence_algorithm) = 'text' AND evidence_algorithm = 'evidence_identity_v1'),
    final_review_round INTEGER NOT NULL
        CHECK (typeof(final_review_round) = 'integer' AND final_review_round BETWEEN 1 AND 3),
    final_review_event_id INTEGER NOT NULL
        CHECK (typeof(final_review_event_id) = 'integer' AND final_review_event_id > 0),
    workspace_generation INTEGER NOT NULL
        CHECK (
            typeof(workspace_generation) = 'integer'
            AND workspace_generation BETWEEN 0 AND 9007199254740991
        ),
    workspace_fingerprint TEXT NOT NULL
        CHECK (
            typeof(workspace_fingerprint) = 'text'
            AND length(CAST(workspace_fingerprint AS BLOB)) = 64
            AND workspace_fingerprint NOT GLOB '*[^0-9a-f]*'
        ),
    checks_digest TEXT NOT NULL
        CHECK (
            typeof(checks_digest) = 'text'
            AND length(CAST(checks_digest AS BLOB)) = 64
            AND checks_digest NOT GLOB '*[^0-9a-f]*'
        ),
    coverage_digest TEXT NOT NULL
        CHECK (
            typeof(coverage_digest) = 'text'
            AND length(CAST(coverage_digest AS BLOB)) = 64
            AND coverage_digest NOT GLOB '*[^0-9a-f]*'
        ),
    artifact_base_commit TEXT NOT NULL
        CHECK (
            typeof(artifact_base_commit) = 'text'
            AND length(CAST(artifact_base_commit AS BLOB)) IN (40, 64)
            AND artifact_base_commit NOT GLOB '*[^0-9a-f]*'
        ),
    artifact_source_branch TEXT NOT NULL
        CHECK (
            typeof(artifact_source_branch) = 'text'
            AND substr(artifact_source_branch, 1, 11) = 'refs/heads/'
            AND length(CAST(artifact_source_branch AS BLOB)) BETWEEN 12 AND 4096
            AND substr(artifact_source_branch, 12, 1) != '-'
            AND substr(artifact_source_branch, -1, 1) NOT IN ('/', '.')
            AND instr(artifact_source_branch, '..') = 0
            AND instr(artifact_source_branch, '@{') = 0
            AND instr(artifact_source_branch, '//') = 0
            AND instr(artifact_source_branch, ' ') = 0
            AND instr(artifact_source_branch, '~') = 0
            AND instr(artifact_source_branch, '^') = 0
            AND instr(artifact_source_branch, ':') = 0
            AND instr(artifact_source_branch, '?') = 0
            AND instr(artifact_source_branch, '*') = 0
            AND instr(artifact_source_branch, '[') = 0
            AND instr(artifact_source_branch, '\') = 0
            AND instr(artifact_source_branch, char(0)) = 0
            AND substr(artifact_source_branch, 12) NOT GLOB '.*'
            AND instr(substr(artifact_source_branch, 12), '/.') = 0
            AND substr(artifact_source_branch, -5) != '.lock'
            AND instr(substr(artifact_source_branch, 12), '.lock/') = 0
        ),
    artifact_worktree_path TEXT NOT NULL
        CHECK (
            typeof(artifact_worktree_path) = 'text'
            AND length(CAST(artifact_worktree_path AS BLOB)) BETWEEN 1 AND 32768
        ),
    common_git_identity_algorithm TEXT NOT NULL
        CHECK (
            typeof(common_git_identity_algorithm) = 'text'
            AND common_git_identity_algorithm = 'directory_identity_v1'
        ),
    common_git_identity_digest TEXT NOT NULL
        CHECK (
            typeof(common_git_identity_digest) = 'text'
            AND length(CAST(common_git_identity_digest AS BLOB)) = 64
            AND common_git_identity_digest NOT GLOB '*[^0-9a-f]*'
        ),
    worktree_admin_identity_algorithm TEXT NOT NULL
        CHECK (
            typeof(worktree_admin_identity_algorithm) = 'text'
            AND worktree_admin_identity_algorithm = 'directory_identity_v1'
        ),
    worktree_admin_identity_digest TEXT NOT NULL
        CHECK (
            typeof(worktree_admin_identity_digest) = 'text'
            AND length(CAST(worktree_admin_identity_digest AS BLOB)) = 64
            AND worktree_admin_identity_digest NOT GLOB '*[^0-9a-f]*'
        ),
    fixed_lock_reason TEXT NOT NULL
        CHECK (typeof(fixed_lock_reason) = 'text' AND fixed_lock_reason = 'codex-reserved'),
    candidate_tree_oid TEXT
        CHECK (
            candidate_tree_oid IS NULL
            OR (
                typeof(candidate_tree_oid) = 'text'
                AND length(CAST(candidate_tree_oid AS BLOB)) IN (40, 64)
                AND candidate_tree_oid NOT GLOB '*[^0-9a-f]*'
            )
        ),
    preflight_source_commit_oid TEXT
        CHECK (
            preflight_source_commit_oid IS NULL
            OR (
                typeof(preflight_source_commit_oid) = 'text'
                AND length(CAST(preflight_source_commit_oid AS BLOB)) IN (40, 64)
                AND preflight_source_commit_oid NOT GLOB '*[^0-9a-f]*'
            )
        ),
    delivery_source_task_id TEXT
        CHECK (
            delivery_source_task_id IS NULL
            OR (
                typeof(delivery_source_task_id) = 'text'
                AND length(CAST(delivery_source_task_id AS BLOB)) = 36
                AND substr(delivery_source_task_id, 9, 1) = '-'
                AND substr(delivery_source_task_id, 14, 1) = '-'
                AND substr(delivery_source_task_id, 19, 1) = '-'
                AND substr(delivery_source_task_id, 24, 1) = '-'
                AND length(CAST(replace(delivery_source_task_id, '-', '') AS BLOB)) = 32
                AND replace(delivery_source_task_id, '-', '') NOT GLOB '*[^0-9a-f]*'
                AND delivery_source_task_id != '00000000-0000-0000-0000-000000000000'
            )
        ),
    source_commit_oid TEXT
        CHECK (
            source_commit_oid IS NULL
            OR (
                typeof(source_commit_oid) = 'text'
                AND length(CAST(source_commit_oid AS BLOB)) IN (40, 64)
                AND source_commit_oid NOT GLOB '*[^0-9a-f]*'
            )
        ),
    preflight_receipt_id TEXT NOT NULL
        CHECK (
            typeof(preflight_receipt_id) = 'text'
            AND length(CAST(preflight_receipt_id AS BLOB)) = 36
            AND substr(preflight_receipt_id, 9, 1) = '-'
            AND substr(preflight_receipt_id, 14, 1) = '-'
            AND substr(preflight_receipt_id, 19, 1) = '-'
            AND substr(preflight_receipt_id, 24, 1) = '-'
            AND length(CAST(replace(preflight_receipt_id, '-', '') AS BLOB)) = 32
            AND replace(preflight_receipt_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND preflight_receipt_id != '00000000-0000-0000-0000-000000000000'
        ),
    accept_receipt_id TEXT
        CHECK (
            accept_receipt_id IS NULL
            OR (
                typeof(accept_receipt_id) = 'text'
                AND length(CAST(accept_receipt_id AS BLOB)) = 36
                AND substr(accept_receipt_id, 9, 1) = '-'
                AND substr(accept_receipt_id, 14, 1) = '-'
                AND substr(accept_receipt_id, 19, 1) = '-'
                AND substr(accept_receipt_id, 24, 1) = '-'
                AND length(CAST(replace(accept_receipt_id, '-', '') AS BLOB)) = 32
                AND replace(accept_receipt_id, '-', '') NOT GLOB '*[^0-9a-f]*'
                AND accept_receipt_id != '00000000-0000-0000-0000-000000000000'
            )
        ),
    target_branch TEXT NOT NULL
        CHECK (
            typeof(target_branch) = 'text'
            AND substr(target_branch, 1, 11) = 'refs/heads/'
            AND length(CAST(target_branch AS BLOB)) BETWEEN 12 AND 4096
            AND substr(target_branch, 12, 1) != '-'
            AND substr(target_branch, -1, 1) NOT IN ('/', '.')
            AND instr(target_branch, '..') = 0
            AND instr(target_branch, '@{') = 0
            AND instr(target_branch, '//') = 0
            AND instr(target_branch, ' ') = 0
            AND instr(target_branch, '~') = 0
            AND instr(target_branch, '^') = 0
            AND instr(target_branch, ':') = 0
            AND instr(target_branch, '?') = 0
            AND instr(target_branch, '*') = 0
            AND instr(target_branch, '[') = 0
            AND instr(target_branch, '\') = 0
            AND instr(target_branch, char(0)) = 0
            AND substr(target_branch, 12) NOT GLOB '.*'
            AND instr(substr(target_branch, 12), '/.') = 0
            AND substr(target_branch, -5) != '.lock'
            AND instr(substr(target_branch, 12), '.lock/') = 0
        ),
    expected_target_head TEXT NOT NULL
        CHECK (
            typeof(expected_target_head) = 'text'
            AND length(CAST(expected_target_head AS BLOB)) IN (40, 64)
            AND expected_target_head NOT GLOB '*[^0-9a-f]*'
        ),
    config_attributes_digest TEXT NOT NULL
        CHECK (
            typeof(config_attributes_digest) = 'text'
            AND length(CAST(config_attributes_digest AS BLOB)) = 64
            AND config_attributes_digest NOT GLOB '*[^0-9a-f]*'
        ),
    target_config_attributes_digest TEXT NOT NULL
        CHECK (
            typeof(target_config_attributes_digest) = 'text'
            AND length(CAST(target_config_attributes_digest AS BLOB)) = 64
            AND target_config_attributes_digest NOT GLOB '*[^0-9a-f]*'
        ),
    target_security_digest TEXT NOT NULL
        CHECK (
            typeof(target_security_digest) = 'text'
            AND length(CAST(target_security_digest AS BLOB)) = 64
            AND target_security_digest NOT GLOB '*[^0-9a-f]*'
        ),
    merge_base_oid TEXT
        CHECK (
            merge_base_oid IS NULL
            OR (
                typeof(merge_base_oid) = 'text'
                AND length(CAST(merge_base_oid AS BLOB)) IN (40, 64)
                AND merge_base_oid NOT GLOB '*[^0-9a-f]*'
            )
        ),
    candidate_merge_tree_oid TEXT
        CHECK (
            candidate_merge_tree_oid IS NULL
            OR (
                typeof(candidate_merge_tree_oid) = 'text'
                AND length(CAST(candidate_merge_tree_oid AS BLOB)) IN (40, 64)
                AND candidate_merge_tree_oid NOT GLOB '*[^0-9a-f]*'
            )
        ),
    conflict_path_count INTEGER
        CHECK (
            conflict_path_count IS NULL
            OR (
                typeof(conflict_path_count) = 'integer'
                AND conflict_path_count BETWEEN 0 AND 128
            )
        ),
    merge_author_name TEXT
        CHECK (merge_author_name IS NULL OR (typeof(merge_author_name) = 'text' AND merge_author_name = 'Coding Agent')),
    merge_author_email TEXT
        CHECK (merge_author_email IS NULL OR (typeof(merge_author_email) = 'text' AND merge_author_email = 'coding-agent@localhost')),
    merge_committer_name TEXT
        CHECK (merge_committer_name IS NULL OR (typeof(merge_committer_name) = 'text' AND merge_committer_name = 'Coding Agent')),
    merge_committer_email TEXT
        CHECK (merge_committer_email IS NULL OR (typeof(merge_committer_email) = 'text' AND merge_committer_email = 'coding-agent@localhost')),
    merge_author_date_bytes TEXT
        CHECK (
            merge_author_date_bytes IS NULL
            OR (
                typeof(merge_author_date_bytes) = 'text'
                AND length(CAST(merge_author_date_bytes AS BLOB)) BETWEEN 7 AND 64
                AND substr(merge_author_date_bytes, -6) = ' +0000'
                AND CAST(
                    CAST(
                        substr(
                            merge_author_date_bytes,
                            1,
                            length(merge_author_date_bytes) - 6
                        ) AS INTEGER
                    ) AS TEXT
                ) = substr(
                    merge_author_date_bytes,
                    1,
                    length(merge_author_date_bytes) - 6
                )
            )
        ),
    merge_committer_date_bytes TEXT
        CHECK (
            merge_committer_date_bytes IS NULL
            OR (
                typeof(merge_committer_date_bytes) = 'text'
                AND length(CAST(merge_committer_date_bytes AS BLOB)) BETWEEN 7 AND 64
                AND substr(merge_committer_date_bytes, -6) = ' +0000'
                AND CAST(
                    CAST(
                        substr(
                            merge_committer_date_bytes,
                            1,
                            length(merge_committer_date_bytes) - 6
                        ) AS INTEGER
                    ) AS TEXT
                ) = substr(
                    merge_committer_date_bytes,
                    1,
                    length(merge_committer_date_bytes) - 6
                )
            )
        ),
    merge_message_template_version INTEGER
        CHECK (
            merge_message_template_version IS NULL
            OR (
                typeof(merge_message_template_version) = 'integer'
                AND merge_message_template_version = 1
            )
        ),
    merge_message_bytes BLOB
        CHECK (
            merge_message_bytes IS NULL
            OR (typeof(merge_message_bytes) = 'blob' AND length(merge_message_bytes) BETWEEN 1 AND 512)
        ),
    expected_merge_commit_oid TEXT
        CHECK (
            expected_merge_commit_oid IS NULL
            OR (
                typeof(expected_merge_commit_oid) = 'text'
                AND length(CAST(expected_merge_commit_oid AS BLOB)) IN (40, 64)
                AND expected_merge_commit_oid NOT GLOB '*[^0-9a-f]*'
            )
        ),
    abort_child_receipt_id TEXT
        CHECK (
            abort_child_receipt_id IS NULL
            OR (
                typeof(abort_child_receipt_id) = 'text'
                AND length(CAST(abort_child_receipt_id AS BLOB)) = 36
                AND substr(abort_child_receipt_id, 9, 1) = '-'
                AND substr(abort_child_receipt_id, 14, 1) = '-'
                AND substr(abort_child_receipt_id, 19, 1) = '-'
                AND substr(abort_child_receipt_id, 24, 1) = '-'
                AND length(CAST(replace(abort_child_receipt_id, '-', '') AS BLOB)) = 32
                AND replace(abort_child_receipt_id, '-', '') NOT GLOB '*[^0-9a-f]*'
                AND abort_child_receipt_id != '00000000-0000-0000-0000-000000000000'
            )
        ),
    abort_merge_head_oid TEXT
        CHECK (
            abort_merge_head_oid IS NULL
            OR (
                typeof(abort_merge_head_oid) = 'text'
                AND length(CAST(abort_merge_head_oid AS BLOB)) IN (40, 64)
                AND abort_merge_head_oid NOT GLOB '*[^0-9a-f]*'
            )
        ),
    abort_index_stages_digest TEXT
        CHECK (
            abort_index_stages_digest IS NULL
            OR (
                typeof(abort_index_stages_digest) = 'text'
                AND length(CAST(abort_index_stages_digest AS BLOB)) = 64
                AND abort_index_stages_digest NOT GLOB '*[^0-9a-f]*'
            )
        ),
    abort_worktree_digest TEXT
        CHECK (
            abort_worktree_digest IS NULL
            OR (
                typeof(abort_worktree_digest) = 'text'
                AND length(CAST(abort_worktree_digest AS BLOB)) = 64
                AND abort_worktree_digest NOT GLOB '*[^0-9a-f]*'
            )
        ),
    abort_merge_autostash_proof TEXT
        CHECK (
            abort_merge_autostash_proof IS NULL
            OR (
                typeof(abort_merge_autostash_proof) = 'text'
                AND abort_merge_autostash_proof = 'absent'
            )
        ),
    merged_disposition_task_id TEXT
        CHECK (
            merged_disposition_task_id IS NULL
            OR (
                typeof(merged_disposition_task_id) = 'text'
                AND length(CAST(merged_disposition_task_id AS BLOB)) = 36
                AND substr(merged_disposition_task_id, 9, 1) = '-'
                AND substr(merged_disposition_task_id, 14, 1) = '-'
                AND substr(merged_disposition_task_id, 19, 1) = '-'
                AND substr(merged_disposition_task_id, 24, 1) = '-'
                AND length(CAST(replace(merged_disposition_task_id, '-', '') AS BLOB)) = 32
                AND replace(merged_disposition_task_id, '-', '') NOT GLOB '*[^0-9a-f]*'
                AND merged_disposition_task_id != '00000000-0000-0000-0000-000000000000'
            )
        ),
    state TEXT NOT NULL
        CHECK (
            typeof(state) = 'text'
            AND state IN (
                'preflight_pending', 'preflight_ready', 'accepted',
                'merge_pending', 'merged', 'abort_pending', 'conflict',
                'rejected', 'stale', 'superseded', 'failed',
                'reconciliation_required'
            )
        ),
    merge_reconciliation_key TEXT NOT NULL
        GENERATED ALWAYS AS (
            CASE
                WHEN state = 'reconciliation_required' THEN 'task:' || task_id
                ELSE 'operation:' || operation_id
            END
        ) STORED,
    failure_code TEXT
        CHECK (
            failure_code IS NULL
            OR (
                typeof(failure_code) = 'text'
                AND length(CAST(failure_code AS BLOB)) BETWEEN 1 AND 128
                AND substr(failure_code, 1, 1) GLOB '[A-Z]'
                AND substr(failure_code, -1, 1) GLOB '[A-Z0-9]'
                AND failure_code NOT GLOB '*[^A-Z0-9_]*'
            )
        ),
    version INTEGER NOT NULL
        CHECK (typeof(version) = 'integer' AND version BETWEEN 1 AND 9007199254740991),
    created_at TEXT NOT NULL
        CHECK (
            typeof(created_at) = 'text'
            AND length(CAST(created_at AS BLOB)) = 30
            AND created_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(created_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19), '+0 seconds')
                = substr(created_at, 1, 19)
        ),
    updated_at TEXT NOT NULL
        CHECK (
            typeof(updated_at) = 'text'
            AND length(CAST(updated_at AS BLOB)) = 30
            AND updated_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(updated_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(updated_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(updated_at, 1, 19), '+0 seconds')
                = substr(updated_at, 1, 19)
        ),
    UNIQUE (preflight_receipt_id),
    UNIQUE (accept_receipt_id),
    UNIQUE (merged_disposition_task_id, operation_id),
    UNIQUE (merge_reconciliation_key),
    FOREIGN KEY (task_id, repository_id, attempt)
        REFERENCES tasks (id, repository_id, attempt),
    FOREIGN KEY (repository_id, task_id, attempt)
        REFERENCES task_attempt_artifacts (repository_id, task_id, attempt),
    FOREIGN KEY (final_review_event_id)
        REFERENCES task_review_evidence (event_id),
    FOREIGN KEY (delivery_source_task_id, source_commit_oid)
        REFERENCES task_delivery_sources (task_id, expected_source_commit_oid)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (operation_id, preflight_receipt_id)
        REFERENCES task_delivery_command_receipts (merge_operation_id, client_request_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (operation_id, accept_receipt_id)
        REFERENCES task_delivery_command_receipts (merge_operation_id, client_request_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (merged_disposition_task_id, operation_id)
        REFERENCES task_artifact_dispositions (task_id, merged_operation_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        preflight_source_commit_oid IS NULL
        OR artifact_base_commit != preflight_source_commit_oid
    ),
    CHECK (artifact_source_branch != target_branch),
    CHECK (
        (candidate_tree_oid IS NULL OR length(CAST(candidate_tree_oid AS BLOB))
            = length(CAST(artifact_base_commit AS BLOB)))
        AND (preflight_source_commit_oid IS NULL
            OR length(CAST(preflight_source_commit_oid AS BLOB))
                = length(CAST(artifact_base_commit AS BLOB)))
        AND length(CAST(expected_target_head AS BLOB))
            = length(CAST(artifact_base_commit AS BLOB))
        AND (
            merge_base_oid IS NULL
            OR length(CAST(merge_base_oid AS BLOB))
                = length(CAST(artifact_base_commit AS BLOB))
        )
        AND (
            candidate_merge_tree_oid IS NULL
            OR length(CAST(candidate_merge_tree_oid AS BLOB))
                = length(CAST(artifact_base_commit AS BLOB))
        )
        AND (
            source_commit_oid IS NULL
            OR length(CAST(source_commit_oid AS BLOB))
                = length(CAST(artifact_base_commit AS BLOB))
        )
        AND (
            expected_merge_commit_oid IS NULL
            OR length(CAST(expected_merge_commit_oid AS BLOB))
                = length(CAST(artifact_base_commit AS BLOB))
        )
        AND (
            abort_merge_head_oid IS NULL
            OR length(CAST(abort_merge_head_oid AS BLOB))
                = length(CAST(artifact_base_commit AS BLOB))
        )
    ),
    CHECK (
        (
            state = 'preflight_pending'
            AND version = 1
            AND candidate_tree_oid IS NULL
            AND preflight_source_commit_oid IS NULL
        )
        OR (
            state IN ('rejected', 'stale', 'reconciliation_required')
            AND version = 2
            AND candidate_tree_oid IS NULL
            AND preflight_source_commit_oid IS NULL
        )
        OR (
            candidate_tree_oid IS NOT NULL
            AND preflight_source_commit_oid IS NOT NULL
            AND (state != 'preflight_pending' OR version = 2)
            AND NOT (
                state IN ('rejected', 'stale', 'reconciliation_required')
                AND version = 2
            )
        )
    ),
    CHECK (
        (delivery_source_task_id IS NULL AND source_commit_oid IS NULL)
        OR (
            delivery_source_task_id = task_id
            AND source_commit_oid IS NOT NULL
        )
    ),
    CHECK (source_commit_oid IS NULL OR source_commit_oid != expected_target_head),
    CHECK (
        expected_merge_commit_oid IS NULL
        OR (
            expected_merge_commit_oid != expected_target_head
            AND (
                source_commit_oid IS NULL
                OR expected_merge_commit_oid != source_commit_oid
            )
        )
    ),
    CHECK (
        (merge_base_oid IS NULL AND candidate_merge_tree_oid IS NULL)
        OR (merge_base_oid IS NOT NULL AND candidate_merge_tree_oid IS NOT NULL)
    ),
    CHECK (accept_receipt_id IS NULL OR accept_receipt_id != preflight_receipt_id),
    CHECK (
        (state IN (
            'preflight_pending', 'preflight_ready', 'accepted',
            'merge_pending', 'merged', 'abort_pending', 'superseded'
        ) AND failure_code IS NULL)
        OR (state = 'conflict' AND failure_code IS NOT NULL
            AND failure_code = 'MERGE_CONFLICT')
        OR (state = 'rejected' AND failure_code IS NOT NULL AND failure_code IN (
            'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED',
            'TARGET_BRANCH_MISMATCH', 'TARGET_WORKTREE_DIRTY',
            'TARGET_IGNORED_PATH_COLLISION', 'TARGET_GIT_OPERATION_IN_PROGRESS',
            'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES',
            'SOURCE_ALREADY_IN_TARGET'
        ))
        OR (state = 'stale' AND failure_code IS NOT NULL AND failure_code IN (
            'DELIVERY_EVIDENCE_STALE', 'TARGET_BRANCH_MISMATCH',
            'TARGET_HEAD_CHANGED', 'DELIVERY_SOURCE_CHANGED'
        ))
        OR (state = 'failed' AND failure_code IS NOT NULL AND failure_code IN (
            'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED',
            'TARGET_BRANCH_MISMATCH', 'TARGET_WORKTREE_DIRTY',
            'TARGET_IGNORED_PATH_COLLISION', 'TARGET_GIT_OPERATION_IN_PROGRESS',
            'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES',
            'SOURCE_ALREADY_IN_TARGET', 'TARGET_HEAD_CHANGED', 'COMMAND_TIMED_OUT'
        ))
        OR (state = 'reconciliation_required' AND failure_code IS NOT NULL
            AND failure_code IN (
            'DELIVERY_RECONCILIATION_REQUIRED', 'DELIVERY_SOURCE_INCONSISTENT',
            'PROCESS_TREE_CLEANUP_FAILED', 'WORKTREE_IDENTITY_MISMATCH',
            'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES'
        ))
    ),
    CHECK (
        (state IN (
            'preflight_pending', 'preflight_ready', 'rejected', 'stale', 'superseded'
        ) AND accept_receipt_id IS NULL AND delivery_source_task_id IS NULL)
        OR state IN (
            'accepted', 'merge_pending', 'merged', 'abort_pending', 'conflict',
            'failed', 'reconciliation_required'
        )
    ),
    CHECK (
        state NOT IN (
            'preflight_ready', 'accepted', 'merge_pending', 'merged', 'abort_pending', 'conflict'
        )
        OR (merge_base_oid IS NOT NULL AND candidate_merge_tree_oid IS NOT NULL)
    ),
    CHECK (
        state NOT IN ('accepted', 'merge_pending', 'merged', 'abort_pending', 'failed')
        OR (
            accept_receipt_id IS NOT NULL
            AND merge_author_name IS NOT NULL
            AND merge_author_email IS NOT NULL
            AND merge_committer_name IS NOT NULL
            AND merge_committer_email IS NOT NULL
            AND merge_author_date_bytes IS NOT NULL
            AND merge_committer_date_bytes IS NOT NULL
            AND merge_author_date_bytes = merge_committer_date_bytes
            AND merge_message_template_version = 1
            AND merge_message_bytes IS NOT NULL
        )
    ),
    CHECK (
        state NOT IN ('merge_pending', 'merged', 'abort_pending', 'failed')
        OR (delivery_source_task_id IS NOT NULL AND source_commit_oid IS NOT NULL)
    ),
    CHECK (
        state NOT IN ('merge_pending', 'merged', 'abort_pending')
        OR expected_merge_commit_oid IS NOT NULL
    ),
    CHECK (
        (
            abort_child_receipt_id IS NULL
            AND abort_merge_head_oid IS NULL
            AND abort_index_stages_digest IS NULL
            AND abort_worktree_digest IS NULL
            AND abort_merge_autostash_proof IS NULL
        )
        OR (
            abort_child_receipt_id IS NOT NULL
            AND abort_merge_head_oid IS NOT NULL
            AND abort_index_stages_digest IS NOT NULL
            AND abort_worktree_digest IS NOT NULL
            AND abort_merge_autostash_proof = 'absent'
        )
    ),
    CHECK (
        state != 'abort_pending'
        OR (
            abort_child_receipt_id IS NOT NULL
            AND abort_merge_head_oid = source_commit_oid
        )
    ),
    CHECK (
        abort_child_receipt_id IS NULL
        OR (
            conflict_path_count IS NOT NULL
            AND conflict_path_count BETWEEN 1 AND 128
        )
    ),
    CHECK (
        (state = 'merged' AND merged_disposition_task_id = task_id)
        OR (state != 'merged' AND merged_disposition_task_id IS NULL)
    ),
    CHECK (
        (
            conflict_path_count IS NOT NULL
            AND (
                state IN ('abort_pending', 'conflict')
                OR (
                    state = 'reconciliation_required'
                    AND abort_child_receipt_id IS NOT NULL
                )
            )
        )
        OR (
            conflict_path_count IS NULL
            AND state NOT IN ('abort_pending', 'conflict')
        )
    ),
    CHECK (
        artifact_base_commit GLOB '*[1-9a-f]*'
        AND (candidate_tree_oid IS NULL OR candidate_tree_oid GLOB '*[1-9a-f]*')
        AND (
            preflight_source_commit_oid IS NULL
            OR preflight_source_commit_oid GLOB '*[1-9a-f]*'
        )
        AND expected_target_head GLOB '*[1-9a-f]*'
        AND (source_commit_oid IS NULL OR source_commit_oid GLOB '*[1-9a-f]*')
        AND (merge_base_oid IS NULL OR merge_base_oid GLOB '*[1-9a-f]*')
        AND (
            candidate_merge_tree_oid IS NULL
            OR candidate_merge_tree_oid GLOB '*[1-9a-f]*'
        )
        AND (
            expected_merge_commit_oid IS NULL
            OR expected_merge_commit_oid GLOB '*[1-9a-f]*'
        )
        AND (
            abort_merge_head_oid IS NULL
            OR abort_merge_head_oid GLOB '*[1-9a-f]*'
        )
    ),
    CHECK (
        (
            merge_author_name IS NULL
            AND merge_author_email IS NULL
            AND merge_committer_name IS NULL
            AND merge_committer_email IS NULL
            AND merge_author_date_bytes IS NULL
            AND merge_committer_date_bytes IS NULL
            AND merge_message_template_version IS NULL
            AND merge_message_bytes IS NULL
        )
        OR (
            merge_author_name IS NOT NULL
            AND merge_author_email IS NOT NULL
            AND merge_committer_name IS NOT NULL
            AND merge_committer_email IS NOT NULL
            AND merge_author_date_bytes IS NOT NULL
            AND merge_committer_date_bytes IS NOT NULL
            AND merge_author_date_bytes = merge_committer_date_bytes
            AND merge_message_template_version = 1
            AND merge_message_bytes IS NOT NULL
            AND state IN (
                'accepted', 'merge_pending', 'merged', 'abort_pending',
                'conflict', 'failed', 'reconciliation_required'
            )
        )
    ),
    CHECK (
        instr(CAST(operation_id AS BLOB), x'00') = 0
        AND instr(CAST(task_id AS BLOB), x'00') = 0
        AND instr(CAST(repository_id AS BLOB), x'00') = 0
        AND instr(CAST(evidence_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(workspace_fingerprint AS BLOB), x'00') = 0
        AND instr(CAST(checks_digest AS BLOB), x'00') = 0
        AND instr(CAST(coverage_digest AS BLOB), x'00') = 0
        AND instr(CAST(artifact_base_commit AS BLOB), x'00') = 0
        AND instr(CAST(artifact_source_branch AS BLOB), x'00') = 0
        AND instr(CAST(artifact_worktree_path AS BLOB), x'00') = 0
        AND instr(CAST(common_git_identity_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(common_git_identity_digest AS BLOB), x'00') = 0
        AND instr(CAST(worktree_admin_identity_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(worktree_admin_identity_digest AS BLOB), x'00') = 0
        AND instr(CAST(fixed_lock_reason AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(candidate_tree_oid, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(preflight_source_commit_oid, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(delivery_source_task_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(source_commit_oid, '') AS BLOB), x'00') = 0
        AND instr(CAST(preflight_receipt_id AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(accept_receipt_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(target_branch AS BLOB), x'00') = 0
        AND instr(CAST(expected_target_head AS BLOB), x'00') = 0
        AND instr(CAST(config_attributes_digest AS BLOB), x'00') = 0
        AND instr(CAST(target_config_attributes_digest AS BLOB), x'00') = 0
        AND instr(CAST(target_security_digest AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merge_base_oid, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(candidate_merge_tree_oid, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merge_author_name, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merge_author_email, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merge_committer_name, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merge_committer_email, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merge_author_date_bytes, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merge_committer_date_bytes, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(expected_merge_commit_oid, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(abort_child_receipt_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(abort_merge_head_oid, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(abort_index_stages_digest, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(abort_worktree_digest, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(abort_merge_autostash_proof, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merged_disposition_task_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(state AS BLOB), x'00') = 0
        AND instr(CAST(merge_reconciliation_key AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(failure_code, '') AS BLOB), x'00') = 0
        AND instr(CAST(created_at AS BLOB), x'00') = 0
        AND instr(CAST(updated_at AS BLOB), x'00') = 0
    )
) STRICT;

CREATE TABLE task_merge_conflicts (
    operation_id TEXT NOT NULL
        CHECK (
            typeof(operation_id) = 'text'
            AND length(CAST(operation_id AS BLOB)) = 36
            AND substr(operation_id, 9, 1) = '-'
            AND substr(operation_id, 14, 1) = '-'
            AND substr(operation_id, 19, 1) = '-'
            AND substr(operation_id, 24, 1) = '-'
            AND length(CAST(replace(operation_id, '-', '') AS BLOB)) = 32
            AND replace(operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND operation_id != '00000000-0000-0000-0000-000000000000'
        ),
    ordinal INTEGER NOT NULL
        CHECK (typeof(ordinal) = 'integer' AND ordinal BETWEEN 0 AND 127),
    path_encoding TEXT NOT NULL
        CHECK (typeof(path_encoding) = 'text' AND path_encoding IN ('utf8', 'base64url')),
    path_value TEXT NOT NULL
        CHECK (
            typeof(path_value) = 'text'
            AND length(CAST(path_value AS BLOB)) BETWEEN 1 AND 4096
        ),
    PRIMARY KEY (operation_id, ordinal),
    FOREIGN KEY (operation_id) REFERENCES task_merge_operations (operation_id),
    CHECK (path_encoding != 'base64url' OR path_value NOT GLOB '*[^A-Za-z0-9_-]*'),
    CHECK (
        instr(CAST(operation_id AS BLOB), x'00') = 0
        AND instr(CAST(path_encoding AS BLOB), x'00') = 0
        AND instr(CAST(path_value AS BLOB), x'00') = 0
    )
) STRICT;

CREATE TABLE task_artifact_dispositions (
    task_id TEXT PRIMARY KEY NOT NULL
        CHECK (
            typeof(task_id) = 'text'
            AND length(CAST(task_id AS BLOB)) = 36
            AND substr(task_id, 9, 1) = '-'
            AND substr(task_id, 14, 1) = '-'
            AND substr(task_id, 19, 1) = '-'
            AND substr(task_id, 24, 1) = '-'
            AND length(CAST(replace(task_id, '-', '') AS BLOB)) = 32
            AND replace(task_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND task_id != '00000000-0000-0000-0000-000000000000'
        ),
    repository_id TEXT NOT NULL
        CHECK (
            typeof(repository_id) = 'text'
            AND length(CAST(repository_id AS BLOB)) = 36
            AND substr(repository_id, 9, 1) = '-'
            AND substr(repository_id, 14, 1) = '-'
            AND substr(repository_id, 19, 1) = '-'
            AND substr(repository_id, 24, 1) = '-'
            AND length(CAST(replace(repository_id, '-', '') AS BLOB)) = 32
            AND replace(repository_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND repository_id != '00000000-0000-0000-0000-000000000000'
        ),
    attempt INTEGER NOT NULL
        CHECK (typeof(attempt) = 'integer' AND attempt BETWEEN 1 AND 4294967295),
    merged_operation_id TEXT NOT NULL
        CHECK (
            typeof(merged_operation_id) = 'text'
            AND length(CAST(merged_operation_id AS BLOB)) = 36
            AND substr(merged_operation_id, 9, 1) = '-'
            AND substr(merged_operation_id, 14, 1) = '-'
            AND substr(merged_operation_id, 19, 1) = '-'
            AND substr(merged_operation_id, 24, 1) = '-'
            AND length(CAST(replace(merged_operation_id, '-', '') AS BLOB)) = 32
            AND replace(merged_operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND merged_operation_id != '00000000-0000-0000-0000-000000000000'
        ),
    delivery_source_task_id TEXT NOT NULL CHECK (delivery_source_task_id = task_id),
    source_commit_oid TEXT NOT NULL
        CHECK (
            typeof(source_commit_oid) = 'text'
            AND length(CAST(source_commit_oid AS BLOB)) IN (40, 64)
            AND source_commit_oid NOT GLOB '*[^0-9a-f]*'
        ),
    worktree_cleanup_operation_id TEXT
        CHECK (
            worktree_cleanup_operation_id IS NULL
            OR (
                typeof(worktree_cleanup_operation_id) = 'text'
                AND length(CAST(worktree_cleanup_operation_id AS BLOB)) = 36
                AND substr(worktree_cleanup_operation_id, 9, 1) = '-'
                AND substr(worktree_cleanup_operation_id, 14, 1) = '-'
                AND substr(worktree_cleanup_operation_id, 19, 1) = '-'
                AND substr(worktree_cleanup_operation_id, 24, 1) = '-'
                AND length(CAST(replace(worktree_cleanup_operation_id, '-', '') AS BLOB)) = 32
                AND replace(worktree_cleanup_operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
                AND worktree_cleanup_operation_id != '00000000-0000-0000-0000-000000000000'
            )
        ),
    worktree_cleanup_operation_version INTEGER
        CHECK (
            worktree_cleanup_operation_version IS NULL
            OR (
                typeof(worktree_cleanup_operation_version) = 'integer'
                AND worktree_cleanup_operation_version BETWEEN 1 AND 9007199254740991
            )
        ),
    worktree_cleanup_operation_state TEXT
        CHECK (
            worktree_cleanup_operation_state IS NULL
            OR (
                typeof(worktree_cleanup_operation_state) = 'text'
                AND worktree_cleanup_operation_state IN (
                    'unlocked_pending_remove', 'completed',
                    'reconciliation_required'
                )
            )
        ),
    worktree_cleanup_entity_kind TEXT
        GENERATED ALWAYS AS (
            CASE
                WHEN worktree_cleanup_operation_id IS NULL THEN NULL
                ELSE 'cleanup_operation'
            END
        ) STORED,
    worktree_cleanup_kind TEXT
        GENERATED ALWAYS AS (
            CASE
                WHEN worktree_cleanup_operation_id IS NULL THEN NULL
                ELSE 'remove_worktree'
            END
        ) STORED,
    branch_cleanup_operation_id TEXT
        CHECK (
            branch_cleanup_operation_id IS NULL
            OR (
                typeof(branch_cleanup_operation_id) = 'text'
                AND length(CAST(branch_cleanup_operation_id AS BLOB)) = 36
                AND substr(branch_cleanup_operation_id, 9, 1) = '-'
                AND substr(branch_cleanup_operation_id, 14, 1) = '-'
                AND substr(branch_cleanup_operation_id, 19, 1) = '-'
                AND substr(branch_cleanup_operation_id, 24, 1) = '-'
                AND length(CAST(replace(branch_cleanup_operation_id, '-', '') AS BLOB)) = 32
                AND replace(branch_cleanup_operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
                AND branch_cleanup_operation_id != '00000000-0000-0000-0000-000000000000'
            )
        ),
    branch_cleanup_operation_version INTEGER
        CHECK (
            branch_cleanup_operation_version IS NULL
            OR (
                typeof(branch_cleanup_operation_version) = 'integer'
                AND branch_cleanup_operation_version BETWEEN 1 AND 9007199254740991
            )
        ),
    branch_cleanup_operation_state TEXT
        CHECK (
            branch_cleanup_operation_state IS NULL
            OR (
                typeof(branch_cleanup_operation_state) = 'text'
                AND branch_cleanup_operation_state IN (
                    'completed', 'reconciliation_required'
                )
            )
        ),
    branch_cleanup_entity_kind TEXT
        GENERATED ALWAYS AS (
            CASE
                WHEN branch_cleanup_operation_id IS NULL THEN NULL
                ELSE 'cleanup_operation'
            END
        ) STORED,
    branch_cleanup_kind TEXT
        GENERATED ALWAYS AS (
            CASE
                WHEN branch_cleanup_operation_id IS NULL THEN NULL
                ELSE 'delete_branch'
            END
        ) STORED,
    worktree_state TEXT NOT NULL
        CHECK (
            typeof(worktree_state) = 'text'
            AND worktree_state IN (
                'retained_locked', 'retained_unlocked', 'removed',
                'reconciliation_required'
            )
        ),
    worktree_version INTEGER NOT NULL
        CHECK (typeof(worktree_version) = 'integer' AND worktree_version BETWEEN 1 AND 9007199254740991),
    worktree_failure_code TEXT
        CHECK (
            worktree_failure_code IS NULL
            OR (
                typeof(worktree_failure_code) = 'text'
                AND length(CAST(worktree_failure_code AS BLOB)) BETWEEN 1 AND 128
                AND substr(worktree_failure_code, 1, 1) GLOB '[A-Z]'
                AND substr(worktree_failure_code, -1, 1) GLOB '[A-Z0-9]'
                AND worktree_failure_code NOT GLOB '*[^A-Z0-9_]*'
            )
        ),
    worktree_updated_at TEXT NOT NULL
        CHECK (
            typeof(worktree_updated_at) = 'text'
            AND length(CAST(worktree_updated_at AS BLOB)) = 30
            AND worktree_updated_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(worktree_updated_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(worktree_updated_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(worktree_updated_at, 1, 19), '+0 seconds')
                = substr(worktree_updated_at, 1, 19)
        ),
    branch_state TEXT NOT NULL
        CHECK (
            typeof(branch_state) = 'text'
            AND branch_state IN ('retained', 'deleted', 'reconciliation_required')
        ),
    branch_version INTEGER NOT NULL
        CHECK (typeof(branch_version) = 'integer' AND branch_version BETWEEN 1 AND 9007199254740991),
    branch_failure_code TEXT
        CHECK (
            branch_failure_code IS NULL
            OR (
                typeof(branch_failure_code) = 'text'
                AND length(CAST(branch_failure_code AS BLOB)) BETWEEN 1 AND 128
                AND substr(branch_failure_code, 1, 1) GLOB '[A-Z]'
                AND substr(branch_failure_code, -1, 1) GLOB '[A-Z0-9]'
                AND branch_failure_code NOT GLOB '*[^A-Z0-9_]*'
            )
        ),
    branch_updated_at TEXT NOT NULL
        CHECK (
            typeof(branch_updated_at) = 'text'
            AND length(CAST(branch_updated_at AS BLOB)) = 30
            AND branch_updated_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(branch_updated_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(branch_updated_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(branch_updated_at, 1, 19), '+0 seconds')
                = substr(branch_updated_at, 1, 19)
        ),
    created_at TEXT NOT NULL
        CHECK (
            typeof(created_at) = 'text'
            AND length(CAST(created_at AS BLOB)) = 30
            AND created_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(created_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19), '+0 seconds')
                = substr(created_at, 1, 19)
        ),
    UNIQUE (task_id, merged_operation_id),
    FOREIGN KEY (task_id, repository_id, attempt)
        REFERENCES tasks (id, repository_id, attempt),
    FOREIGN KEY (repository_id, task_id, attempt)
        REFERENCES task_attempt_artifacts (repository_id, task_id, attempt),
    FOREIGN KEY (delivery_source_task_id, source_commit_oid)
        REFERENCES task_delivery_sources (task_id, expected_source_commit_oid),
    FOREIGN KEY (task_id, merged_operation_id)
        REFERENCES task_merge_operations (merged_disposition_task_id, operation_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        task_id, worktree_cleanup_operation_id, worktree_cleanup_kind
    ) REFERENCES task_cleanup_operations (task_id, operation_id, kind)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        worktree_cleanup_entity_kind, worktree_cleanup_operation_id,
        worktree_cleanup_operation_version, worktree_cleanup_operation_state
    ) REFERENCES task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, to_state
    ) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        task_id, branch_cleanup_operation_id, branch_cleanup_kind
    ) REFERENCES task_cleanup_operations (task_id, operation_id, kind)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        branch_cleanup_entity_kind, branch_cleanup_operation_id,
        branch_cleanup_operation_version, branch_cleanup_operation_state
    ) REFERENCES task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, to_state
    ) DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (worktree_state = 'reconciliation_required' AND worktree_failure_code IS NOT NULL)
        OR (worktree_state != 'reconciliation_required' AND worktree_failure_code IS NULL)
    ),
    CHECK (
        (branch_state = 'reconciliation_required' AND branch_failure_code IS NOT NULL)
        OR (branch_state != 'reconciliation_required' AND branch_failure_code IS NULL)
    ),
    CHECK (
        (
            worktree_state = 'retained_locked'
            AND worktree_cleanup_operation_id IS NULL
            AND worktree_cleanup_operation_version IS NULL
            AND worktree_cleanup_operation_state IS NULL
        )
        OR (
            worktree_state = 'retained_unlocked'
            AND worktree_cleanup_operation_id IS NOT NULL
            AND worktree_cleanup_operation_version IS NOT NULL
            AND worktree_cleanup_operation_state = 'unlocked_pending_remove'
        )
        OR (
            worktree_state = 'removed'
            AND worktree_cleanup_operation_id IS NOT NULL
            AND worktree_cleanup_operation_version IS NOT NULL
            AND worktree_cleanup_operation_state = 'completed'
        )
        OR (
            worktree_state = 'reconciliation_required'
            AND worktree_cleanup_operation_id IS NOT NULL
            AND worktree_cleanup_operation_version IS NOT NULL
            AND worktree_cleanup_operation_state = 'reconciliation_required'
        )
    ),
    CHECK (
        (
            branch_state = 'retained'
            AND branch_cleanup_operation_id IS NULL
            AND branch_cleanup_operation_version IS NULL
            AND branch_cleanup_operation_state IS NULL
        )
        OR (
            branch_state = 'deleted'
            AND branch_cleanup_operation_id IS NOT NULL
            AND branch_cleanup_operation_version IS NOT NULL
            AND branch_cleanup_operation_state = 'completed'
        )
        OR (
            branch_state = 'reconciliation_required'
            AND branch_cleanup_operation_id IS NOT NULL
            AND branch_cleanup_operation_version IS NOT NULL
            AND branch_cleanup_operation_state = 'reconciliation_required'
        )
    ),
    CHECK (source_commit_oid GLOB '*[1-9a-f]*'),
    CHECK (
        instr(CAST(task_id AS BLOB), x'00') = 0
        AND instr(CAST(repository_id AS BLOB), x'00') = 0
        AND instr(CAST(merged_operation_id AS BLOB), x'00') = 0
        AND instr(CAST(delivery_source_task_id AS BLOB), x'00') = 0
        AND instr(CAST(source_commit_oid AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(worktree_cleanup_operation_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(worktree_cleanup_operation_state, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(worktree_cleanup_entity_kind, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(worktree_cleanup_kind, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(branch_cleanup_operation_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(branch_cleanup_operation_state, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(branch_cleanup_entity_kind, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(branch_cleanup_kind, '') AS BLOB), x'00') = 0
        AND instr(CAST(worktree_state AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(worktree_failure_code, '') AS BLOB), x'00') = 0
        AND instr(CAST(worktree_updated_at AS BLOB), x'00') = 0
        AND instr(CAST(branch_state AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(branch_failure_code, '') AS BLOB), x'00') = 0
        AND instr(CAST(branch_updated_at AS BLOB), x'00') = 0
        AND instr(CAST(created_at AS BLOB), x'00') = 0
    )
) STRICT;

CREATE TABLE task_cleanup_operations (
    operation_id TEXT PRIMARY KEY NOT NULL
        CHECK (
            typeof(operation_id) = 'text'
            AND length(CAST(operation_id AS BLOB)) = 36
            AND substr(operation_id, 9, 1) = '-'
            AND substr(operation_id, 14, 1) = '-'
            AND substr(operation_id, 19, 1) = '-'
            AND substr(operation_id, 24, 1) = '-'
            AND length(CAST(replace(operation_id, '-', '') AS BLOB)) = 32
            AND replace(operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND operation_id != '00000000-0000-0000-0000-000000000000'
        ),
    task_id TEXT NOT NULL
        CHECK (
            typeof(task_id) = 'text'
            AND length(CAST(task_id AS BLOB)) = 36
            AND substr(task_id, 9, 1) = '-'
            AND substr(task_id, 14, 1) = '-'
            AND substr(task_id, 19, 1) = '-'
            AND substr(task_id, 24, 1) = '-'
            AND length(CAST(replace(task_id, '-', '') AS BLOB)) = 32
            AND replace(task_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND task_id != '00000000-0000-0000-0000-000000000000'
        ),
    repository_id TEXT NOT NULL
        CHECK (
            typeof(repository_id) = 'text'
            AND length(CAST(repository_id AS BLOB)) = 36
            AND substr(repository_id, 9, 1) = '-'
            AND substr(repository_id, 14, 1) = '-'
            AND substr(repository_id, 19, 1) = '-'
            AND substr(repository_id, 24, 1) = '-'
            AND length(CAST(replace(repository_id, '-', '') AS BLOB)) = 32
            AND replace(repository_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND repository_id != '00000000-0000-0000-0000-000000000000'
        ),
    attempt INTEGER NOT NULL
        CHECK (typeof(attempt) = 'integer' AND attempt BETWEEN 1 AND 4294967295),
    kind TEXT NOT NULL
        CHECK (typeof(kind) = 'text' AND kind IN ('remove_worktree', 'delete_branch')),
    origin_receipt_id TEXT NOT NULL
        CHECK (
            typeof(origin_receipt_id) = 'text'
            AND length(CAST(origin_receipt_id AS BLOB)) = 36
            AND substr(origin_receipt_id, 9, 1) = '-'
            AND substr(origin_receipt_id, 14, 1) = '-'
            AND substr(origin_receipt_id, 19, 1) = '-'
            AND substr(origin_receipt_id, 24, 1) = '-'
            AND length(CAST(replace(origin_receipt_id, '-', '') AS BLOB)) = 32
            AND replace(origin_receipt_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND origin_receipt_id != '00000000-0000-0000-0000-000000000000'
        ),
    disposition_task_id TEXT NOT NULL CHECK (disposition_task_id = task_id),
    expected_worktree_path TEXT NOT NULL
        CHECK (
            typeof(expected_worktree_path) = 'text'
            AND length(CAST(expected_worktree_path AS BLOB)) BETWEEN 1 AND 32768
        ),
    expected_admin_identity_algorithm TEXT NOT NULL
        CHECK (
            typeof(expected_admin_identity_algorithm) = 'text'
            AND expected_admin_identity_algorithm = 'directory_identity_v1'
        ),
    expected_admin_identity_digest TEXT NOT NULL
        CHECK (
            typeof(expected_admin_identity_digest) = 'text'
            AND length(CAST(expected_admin_identity_digest AS BLOB)) = 64
            AND expected_admin_identity_digest NOT GLOB '*[^0-9a-f]*'
        ),
    expected_common_git_identity_algorithm TEXT NOT NULL
        CHECK (
            typeof(expected_common_git_identity_algorithm) = 'text'
            AND expected_common_git_identity_algorithm = 'directory_identity_v1'
        ),
    expected_common_git_identity_digest TEXT NOT NULL
        CHECK (
            typeof(expected_common_git_identity_digest) = 'text'
            AND length(CAST(expected_common_git_identity_digest AS BLOB)) = 64
            AND expected_common_git_identity_digest NOT GLOB '*[^0-9a-f]*'
        ),
    expected_source_ref TEXT NOT NULL
        CHECK (
            typeof(expected_source_ref) = 'text'
            AND substr(expected_source_ref, 1, 11) = 'refs/heads/'
            AND length(CAST(expected_source_ref AS BLOB)) BETWEEN 12 AND 4096
            AND substr(expected_source_ref, 12, 1) != '-'
            AND substr(expected_source_ref, -1, 1) NOT IN ('/', '.')
            AND instr(expected_source_ref, '..') = 0
            AND instr(expected_source_ref, '@{') = 0
            AND instr(expected_source_ref, '//') = 0
            AND instr(expected_source_ref, ' ') = 0
            AND instr(expected_source_ref, '~') = 0
            AND instr(expected_source_ref, '^') = 0
            AND instr(expected_source_ref, ':') = 0
            AND instr(expected_source_ref, '?') = 0
            AND instr(expected_source_ref, '*') = 0
            AND instr(expected_source_ref, '[') = 0
            AND instr(expected_source_ref, '\') = 0
            AND instr(expected_source_ref, char(0)) = 0
            AND substr(expected_source_ref, 12) NOT GLOB '.*'
            AND instr(substr(expected_source_ref, 12), '/.') = 0
            AND substr(expected_source_ref, -5) != '.lock'
            AND instr(substr(expected_source_ref, 12), '.lock/') = 0
        ),
    expected_source_oid TEXT NOT NULL
        CHECK (
            typeof(expected_source_oid) = 'text'
            AND length(CAST(expected_source_oid AS BLOB)) IN (40, 64)
            AND expected_source_oid NOT GLOB '*[^0-9a-f]*'
        ),
    expected_disposition_version INTEGER NOT NULL
        CHECK (
            typeof(expected_disposition_version) = 'integer'
            AND expected_disposition_version BETWEEN 1 AND 9007199254740991
        ),
    expected_target_ref TEXT
        CHECK (
            expected_target_ref IS NULL
            OR (
                typeof(expected_target_ref) = 'text'
                AND substr(expected_target_ref, 1, 11) = 'refs/heads/'
                AND length(CAST(expected_target_ref AS BLOB)) BETWEEN 12 AND 4096
                AND substr(expected_target_ref, 12, 1) != '-'
                AND substr(expected_target_ref, -1, 1) NOT IN ('/', '.')
                AND instr(expected_target_ref, '..') = 0
                AND instr(expected_target_ref, '@{') = 0
                AND instr(expected_target_ref, '//') = 0
                AND instr(expected_target_ref, ' ') = 0
                AND instr(expected_target_ref, '~') = 0
                AND instr(expected_target_ref, '^') = 0
                AND instr(expected_target_ref, ':') = 0
                AND instr(expected_target_ref, '?') = 0
                AND instr(expected_target_ref, '*') = 0
                AND instr(expected_target_ref, '[') = 0
                AND instr(expected_target_ref, '\') = 0
                AND instr(expected_target_ref, char(0)) = 0
                AND substr(expected_target_ref, 12) NOT GLOB '.*'
                AND instr(substr(expected_target_ref, 12), '/.') = 0
                AND substr(expected_target_ref, -5) != '.lock'
                AND instr(substr(expected_target_ref, 12), '.lock/') = 0
            )
        ),
    expected_target_head TEXT
        CHECK (
            expected_target_head IS NULL
            OR (
                typeof(expected_target_head) = 'text'
                AND length(CAST(expected_target_head AS BLOB)) IN (40, 64)
                AND expected_target_head NOT GLOB '*[^0-9a-f]*'
            )
        ),
    origin_target_head TEXT
        CHECK (
            origin_target_head IS NULL
            OR (
                typeof(origin_target_head) = 'text'
                AND length(CAST(origin_target_head AS BLOB)) IN (40, 64)
                AND origin_target_head NOT GLOB '*[^0-9a-f]*'
            )
        ),
    state TEXT NOT NULL
        CHECK (
            typeof(state) = 'text'
            AND state IN (
                'unlock_pending', 'unlocked_pending_remove', 'remove_pending',
                'delete_pending', 'completed', 'failed',
                'reconciliation_required'
            )
        ),
    failure_code TEXT
        CHECK (
            failure_code IS NULL
            OR (
                typeof(failure_code) = 'text'
                AND length(CAST(failure_code AS BLOB)) BETWEEN 1 AND 128
                AND substr(failure_code, 1, 1) GLOB '[A-Z]'
                AND substr(failure_code, -1, 1) GLOB '[A-Z0-9]'
                AND failure_code NOT GLOB '*[^A-Z0-9_]*'
            )
        ),
    version INTEGER NOT NULL
        CHECK (typeof(version) = 'integer' AND version BETWEEN 1 AND 9007199254740991),
    created_at TEXT NOT NULL
        CHECK (
            typeof(created_at) = 'text'
            AND length(CAST(created_at AS BLOB)) = 30
            AND created_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(created_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19), '+0 seconds')
                = substr(created_at, 1, 19)
        ),
    updated_at TEXT NOT NULL
        CHECK (
            typeof(updated_at) = 'text'
            AND length(CAST(updated_at AS BLOB)) = 30
            AND updated_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(updated_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(updated_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(updated_at, 1, 19), '+0 seconds')
                = substr(updated_at, 1, 19)
        ),
    UNIQUE (origin_receipt_id),
    UNIQUE (task_id, operation_id, kind),
    FOREIGN KEY (task_id, repository_id, attempt)
        REFERENCES tasks (id, repository_id, attempt),
    FOREIGN KEY (repository_id, task_id, attempt)
        REFERENCES task_attempt_artifacts (repository_id, task_id, attempt),
    FOREIGN KEY (disposition_task_id)
        REFERENCES task_artifact_dispositions (task_id),
    FOREIGN KEY (operation_id, origin_receipt_id)
        REFERENCES task_delivery_command_receipts (cleanup_operation_id, client_request_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (kind = 'remove_worktree' AND state IN (
            'unlock_pending', 'unlocked_pending_remove', 'remove_pending',
            'completed', 'failed', 'reconciliation_required'
        ))
        OR (kind = 'delete_branch' AND state IN (
            'delete_pending', 'completed', 'failed', 'reconciliation_required'
        ))
    ),
    CHECK (
        (state IN (
            'unlock_pending', 'unlocked_pending_remove',
            'remove_pending', 'delete_pending', 'completed'
        ) AND failure_code IS NULL)
        OR (
            kind = 'remove_worktree'
            AND state = 'failed'
            AND failure_code IN ('TARGET_WORKTREE_DIRTY', 'COMMAND_TIMED_OUT')
        )
        OR (
            kind = 'delete_branch'
            AND state = 'failed'
            AND failure_code IN ('SOURCE_BRANCH_NOT_MERGED', 'COMMAND_TIMED_OUT')
        )
        OR (
            state = 'reconciliation_required'
            AND failure_code IN (
                'DELIVERY_RECONCILIATION_REQUIRED',
                'DELIVERY_SOURCE_INCONSISTENT',
                'PROCESS_TREE_CLEANUP_FAILED',
                'WORKTREE_IDENTITY_MISMATCH',
                'UNSAFE_GIT_CONFIGURATION',
                'UNSUPPORTED_GIT_ATTRIBUTES',
                'COMMAND_TIMED_OUT'
            )
        )
    ),
    CHECK (
        (
            kind = 'remove_worktree'
            AND expected_target_ref IS NULL
            AND expected_target_head IS NULL
            AND origin_target_head IS NULL
        )
        OR (
            kind = 'delete_branch'
            AND expected_target_ref IS NOT NULL
            AND expected_target_head IS NOT NULL
            AND origin_target_head IS NOT NULL
            AND expected_target_ref != expected_source_ref
            AND length(CAST(expected_target_head AS BLOB))
                = length(CAST(expected_source_oid AS BLOB))
            AND length(CAST(origin_target_head AS BLOB))
                = length(CAST(expected_source_oid AS BLOB))
        )
    ),
    CHECK (
        expected_source_oid GLOB '*[1-9a-f]*'
        AND (
            expected_target_head IS NULL
            OR expected_target_head GLOB '*[1-9a-f]*'
        )
        AND (
            origin_target_head IS NULL
            OR origin_target_head GLOB '*[1-9a-f]*'
        )
    ),
    CHECK (
        instr(CAST(operation_id AS BLOB), x'00') = 0
        AND instr(CAST(task_id AS BLOB), x'00') = 0
        AND instr(CAST(repository_id AS BLOB), x'00') = 0
        AND instr(CAST(kind AS BLOB), x'00') = 0
        AND instr(CAST(origin_receipt_id AS BLOB), x'00') = 0
        AND instr(CAST(disposition_task_id AS BLOB), x'00') = 0
        AND instr(CAST(expected_worktree_path AS BLOB), x'00') = 0
        AND instr(CAST(expected_admin_identity_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(expected_admin_identity_digest AS BLOB), x'00') = 0
        AND instr(CAST(expected_common_git_identity_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(expected_common_git_identity_digest AS BLOB), x'00') = 0
        AND instr(CAST(expected_source_ref AS BLOB), x'00') = 0
        AND instr(CAST(expected_source_oid AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(expected_target_ref, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(expected_target_head, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(origin_target_head, '') AS BLOB), x'00') = 0
        AND instr(CAST(state AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(failure_code, '') AS BLOB), x'00') = 0
        AND instr(CAST(created_at AS BLOB), x'00') = 0
        AND instr(CAST(updated_at AS BLOB), x'00') = 0
    )
) STRICT;

CREATE TABLE task_cleanup_target_head_observations (
    cleanup_operation_id TEXT NOT NULL
        CHECK (
            typeof(cleanup_operation_id) = 'text'
            AND length(CAST(cleanup_operation_id AS BLOB)) = 36
            AND substr(cleanup_operation_id, 9, 1) = '-'
            AND substr(cleanup_operation_id, 14, 1) = '-'
            AND substr(cleanup_operation_id, 19, 1) = '-'
            AND substr(cleanup_operation_id, 24, 1) = '-'
            AND length(CAST(replace(cleanup_operation_id, '-', '') AS BLOB)) = 32
            AND replace(cleanup_operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND cleanup_operation_id != '00000000-0000-0000-0000-000000000000'
        ),
    operation_version INTEGER NOT NULL
        CHECK (
            typeof(operation_version) = 'integer'
            AND operation_version BETWEEN 1 AND 9007199254740991
        ),
    target_head TEXT NOT NULL
        CHECK (
            typeof(target_head) = 'text'
            AND length(CAST(target_head AS BLOB)) IN (40, 64)
            AND target_head NOT GLOB '*[^0-9a-f]*'
            AND target_head GLOB '*[1-9a-f]*'
        ),
    observed_at TEXT NOT NULL
        CHECK (
            typeof(observed_at) = 'text'
            AND length(CAST(observed_at AS BLOB)) = 30
            AND observed_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(observed_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(observed_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(observed_at, 1, 19), '+0 seconds')
                = substr(observed_at, 1, 19)
        ),
    transition_entity_kind TEXT NOT NULL
        GENERATED ALWAYS AS ('cleanup_operation') STORED,
    PRIMARY KEY (cleanup_operation_id, operation_version),
    FOREIGN KEY (cleanup_operation_id)
        REFERENCES task_cleanup_operations (operation_id),
    FOREIGN KEY (
        transition_entity_kind, cleanup_operation_id, operation_version
    ) REFERENCES task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version
    ) DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        instr(CAST(cleanup_operation_id AS BLOB), x'00') = 0
        AND instr(CAST(target_head AS BLOB), x'00') = 0
        AND instr(CAST(observed_at AS BLOB), x'00') = 0
    )
) STRICT;

CREATE TABLE task_delivery_command_receipts (
    client_request_id TEXT PRIMARY KEY NOT NULL
        CHECK (
            typeof(client_request_id) = 'text'
            AND length(CAST(client_request_id AS BLOB)) = 36
            AND substr(client_request_id, 9, 1) = '-'
            AND substr(client_request_id, 14, 1) = '-'
            AND substr(client_request_id, 19, 1) = '-'
            AND substr(client_request_id, 24, 1) = '-'
            AND length(CAST(replace(client_request_id, '-', '') AS BLOB)) = 32
            AND replace(client_request_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND client_request_id != '00000000-0000-0000-0000-000000000000'
        ),
    command_kind TEXT NOT NULL
        CHECK (
            typeof(command_kind) = 'text'
            AND command_kind IN (
                'preflight', 'accept_merge', 'remove_worktree', 'delete_branch'
            )
        ),
    task_id TEXT NOT NULL
        CHECK (
            typeof(task_id) = 'text'
            AND length(CAST(task_id AS BLOB)) = 36
            AND substr(task_id, 9, 1) = '-'
            AND substr(task_id, 14, 1) = '-'
            AND substr(task_id, 19, 1) = '-'
            AND substr(task_id, 24, 1) = '-'
            AND length(CAST(replace(task_id, '-', '') AS BLOB)) = 32
            AND replace(task_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND task_id != '00000000-0000-0000-0000-000000000000'
        ),
    repository_id TEXT NOT NULL
        CHECK (
            typeof(repository_id) = 'text'
            AND length(CAST(repository_id AS BLOB)) = 36
            AND substr(repository_id, 9, 1) = '-'
            AND substr(repository_id, 14, 1) = '-'
            AND substr(repository_id, 19, 1) = '-'
            AND substr(repository_id, 24, 1) = '-'
            AND length(CAST(replace(repository_id, '-', '') AS BLOB)) = 32
            AND replace(repository_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND repository_id != '00000000-0000-0000-0000-000000000000'
        ),
    attempt INTEGER NOT NULL
        CHECK (typeof(attempt) = 'integer' AND attempt BETWEEN 1 AND 4294967295),
    request_hash_domain TEXT NOT NULL
        CHECK (
            typeof(request_hash_domain) = 'text'
            AND request_hash_domain = 'coding-agent-delivery-command-request'
        ),
    request_hash_version INTEGER NOT NULL
        CHECK (typeof(request_hash_version) = 'integer' AND request_hash_version = 1),
    request_hash_algorithm TEXT NOT NULL
        CHECK (typeof(request_hash_algorithm) = 'text' AND request_hash_algorithm = 'sha256'),
    canonical_request_hash TEXT NOT NULL
        CHECK (
            typeof(canonical_request_hash) = 'text'
            AND length(CAST(canonical_request_hash AS BLOB)) = 64
            AND canonical_request_hash NOT GLOB '*[^0-9a-f]*'
        ),
    operation_kind TEXT NOT NULL
        CHECK (
            typeof(operation_kind) = 'text'
            AND operation_kind IN ('merge_operation', 'cleanup_operation')
        ),
    operation_id TEXT NOT NULL
        CHECK (
            typeof(operation_id) = 'text'
            AND length(CAST(operation_id AS BLOB)) = 36
            AND substr(operation_id, 9, 1) = '-'
            AND substr(operation_id, 14, 1) = '-'
            AND substr(operation_id, 19, 1) = '-'
            AND substr(operation_id, 24, 1) = '-'
            AND length(CAST(replace(operation_id, '-', '') AS BLOB)) = 32
            AND replace(operation_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            AND operation_id != '00000000-0000-0000-0000-000000000000'
        ),
    merge_operation_id TEXT,
    cleanup_operation_id TEXT,
    cleanup_merged_operation_id TEXT,
    accepted_operation_version INTEGER NOT NULL
        CHECK (
            typeof(accepted_operation_version) = 'integer'
            AND accepted_operation_version BETWEEN 1 AND 9007199254740991
        ),
    accepted_operation_state TEXT NOT NULL CHECK (typeof(accepted_operation_state) = 'text'),
    response_discriminator TEXT NOT NULL
        CHECK (
            typeof(response_discriminator) = 'text'
            AND response_discriminator IN (
                'preflight_created', 'merge_accepted',
                'worktree_cleanup_accepted', 'branch_cleanup_accepted'
            )
        ),
    created_at TEXT NOT NULL
        CHECK (
            typeof(created_at) = 'text'
            AND length(CAST(created_at AS BLOB)) = 30
            AND created_at GLOB '????-??-??T??:??:??.?????????Z'
            AND substr(created_at, 21, 9) NOT GLOB '*[^0-9]*'
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19)) IS NOT NULL
            AND strftime('%Y-%m-%dT%H:%M:%S', substr(created_at, 1, 19), '+0 seconds')
                = substr(created_at, 1, 19)
        ),
    UNIQUE (command_kind, operation_id),
    UNIQUE (merge_operation_id, client_request_id),
    UNIQUE (cleanup_operation_id, client_request_id),
    FOREIGN KEY (task_id, repository_id, attempt)
        REFERENCES tasks (id, repository_id, attempt),
    FOREIGN KEY (merge_operation_id)
        REFERENCES task_merge_operations (operation_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (cleanup_operation_id)
        REFERENCES task_cleanup_operations (operation_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (cleanup_merged_operation_id)
        REFERENCES task_merge_operations (operation_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        operation_kind, operation_id, accepted_operation_version,
        accepted_operation_state
    ) REFERENCES task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, to_state
    ) DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (
            operation_kind = 'merge_operation'
            AND merge_operation_id = operation_id
            AND cleanup_operation_id IS NULL
            AND cleanup_merged_operation_id IS NULL
        )
        OR (
            operation_kind = 'cleanup_operation'
            AND cleanup_operation_id = operation_id
            AND merge_operation_id IS NULL
            AND cleanup_merged_operation_id IS NOT NULL
        )
    ),
    CHECK (
        (
            command_kind = 'preflight'
            AND operation_kind = 'merge_operation'
            AND accepted_operation_state = 'preflight_pending'
            AND response_discriminator = 'preflight_created'
        )
        OR (
            command_kind = 'accept_merge'
            AND operation_kind = 'merge_operation'
            AND accepted_operation_state = 'accepted'
            AND response_discriminator = 'merge_accepted'
        )
        OR (
            command_kind = 'remove_worktree'
            AND operation_kind = 'cleanup_operation'
            AND accepted_operation_state IN ('unlock_pending', 'remove_pending')
            AND response_discriminator = 'worktree_cleanup_accepted'
        )
        OR (
            command_kind = 'delete_branch'
            AND operation_kind = 'cleanup_operation'
            AND accepted_operation_state = 'delete_pending'
            AND response_discriminator = 'branch_cleanup_accepted'
        )
    ),
    CHECK (
        instr(CAST(client_request_id AS BLOB), x'00') = 0
        AND instr(CAST(command_kind AS BLOB), x'00') = 0
        AND instr(CAST(task_id AS BLOB), x'00') = 0
        AND instr(CAST(repository_id AS BLOB), x'00') = 0
        AND instr(CAST(request_hash_domain AS BLOB), x'00') = 0
        AND instr(CAST(request_hash_algorithm AS BLOB), x'00') = 0
        AND instr(CAST(canonical_request_hash AS BLOB), x'00') = 0
        AND instr(CAST(operation_kind AS BLOB), x'00') = 0
        AND instr(CAST(operation_id AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(merge_operation_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(cleanup_operation_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(COALESCE(cleanup_merged_operation_id, '') AS BLOB), x'00') = 0
        AND instr(CAST(accepted_operation_state AS BLOB), x'00') = 0
        AND instr(CAST(response_discriminator AS BLOB), x'00') = 0
        AND instr(CAST(created_at AS BLOB), x'00') = 0
    )
) STRICT;

CREATE UNIQUE INDEX task_merge_operations_one_active
    ON task_merge_operations (task_id)
    WHERE state IN (
        'preflight_pending', 'preflight_ready', 'accepted',
        'merge_pending', 'abort_pending'
    );

CREATE UNIQUE INDEX task_merge_operations_one_merged
    ON task_merge_operations (task_id)
    WHERE state = 'merged';

CREATE UNIQUE INDEX task_merge_operations_abort_child_receipt_unique
    ON task_merge_operations (abort_child_receipt_id)
    WHERE abort_child_receipt_id IS NOT NULL;

CREATE UNIQUE INDEX task_cleanup_operations_one_active_disposition
    ON task_cleanup_operations (disposition_task_id)
    WHERE state IN (
        'unlock_pending', 'unlocked_pending_remove',
        'remove_pending', 'delete_pending'
    );

CREATE INDEX task_delivery_operation_transitions_initial_order
    ON task_delivery_operation_transitions (entity_kind, transition_id)
    WHERE entity_version = 1;

CREATE TRIGGER task_delivery_sources_branch_canonical_on_insert
BEFORE INSERT ON task_delivery_sources
WHEN EXISTS (
    WITH RECURSIVE
    values_to_check(value, reject_controls) AS (
        VALUES
            (NEW.artifact_source_branch, 1),
            (NEW.artifact_worktree_path, 0),
            (NEW.author_date_bytes, 0),
            (NEW.committer_date_bytes, 0)
    ),
    characters(value, reject_controls, position, reencoded_hex, has_control) AS (
        SELECT value, reject_controls, 1, '', 0 FROM values_to_check
        UNION ALL
        SELECT
            value,
            reject_controls,
            position + 1,
            reencoded_hex
                || hex(CAST(char(unicode(substr(value, position, 1))) AS BLOB)),
            CASE
                WHEN has_control = 1 OR (
                    reject_controls = 1
                    AND (
                        unicode(substr(value, position, 1)) < 32
                        OR unicode(substr(value, position, 1)) BETWEEN 127 AND 159
                    )
                ) THEN 1
                ELSE 0
            END
        FROM characters
        WHERE position <= length(value)
    )
    SELECT 1 FROM characters
    WHERE position = length(value) + 1
      AND (has_control = 1 OR reencoded_hex != hex(CAST(value AS BLOB)))
)
BEGIN
    SELECT RAISE(ABORT, 'delivery source text is not canonical');
END;

CREATE TRIGGER task_merge_operations_branches_canonical_on_insert
BEFORE INSERT ON task_merge_operations
WHEN EXISTS (
    WITH RECURSIVE
    values_to_check(value, reject_controls) AS (
        VALUES
            (NEW.artifact_source_branch, 1),
            (NEW.target_branch, 1),
            (NEW.artifact_worktree_path, 0),
            (NEW.merge_author_date_bytes, 0),
            (NEW.merge_committer_date_bytes, 0)
    ),
    characters(value, reject_controls, position, reencoded_hex, has_control) AS (
        SELECT value, reject_controls, 1, '', 0
        FROM values_to_check
        WHERE value IS NOT NULL
        UNION ALL
        SELECT
            value,
            reject_controls,
            position + 1,
            reencoded_hex
                || hex(CAST(char(unicode(substr(value, position, 1))) AS BLOB)),
            CASE
                WHEN has_control = 1 OR (
                    reject_controls = 1
                    AND (
                        unicode(substr(value, position, 1)) < 32
                        OR unicode(substr(value, position, 1)) BETWEEN 127 AND 159
                    )
                ) THEN 1
                ELSE 0
            END
        FROM characters
        WHERE position <= length(value)
    )
    SELECT 1 FROM characters
    WHERE position = length(value) + 1
      AND (has_control = 1 OR reencoded_hex != hex(CAST(value AS BLOB)))
)
BEGIN
    SELECT RAISE(ABORT, 'merge operation text is not canonical');
END;

CREATE TRIGGER task_merge_operations_commit_dates_canonical_on_update
BEFORE UPDATE OF merge_author_date_bytes, merge_committer_date_bytes
ON task_merge_operations
WHEN EXISTS (
    WITH RECURSIVE
    values_to_check(value) AS (
        VALUES (NEW.merge_author_date_bytes), (NEW.merge_committer_date_bytes)
    ),
    characters(value, position, reencoded_hex) AS (
        SELECT value, 1, ''
        FROM values_to_check
        WHERE value IS NOT NULL
        UNION ALL
        SELECT
            value,
            position + 1,
            reencoded_hex
                || hex(CAST(char(unicode(substr(value, position, 1))) AS BLOB))
        FROM characters
        WHERE position <= length(value)
    )
    SELECT 1 FROM characters
    WHERE position = length(value) + 1
      AND reencoded_hex != hex(CAST(value AS BLOB))
)
BEGIN
    SELECT RAISE(ABORT, 'merge commit date text is not canonical');
END;

CREATE TRIGGER task_cleanup_operations_branches_canonical_on_insert
BEFORE INSERT ON task_cleanup_operations
WHEN EXISTS (
    WITH RECURSIVE
    values_to_check(value, reject_controls) AS (
        VALUES
            (NEW.expected_source_ref, 1),
            (NEW.expected_target_ref, 1),
            (NEW.expected_worktree_path, 0)
    ),
    characters(value, reject_controls, position, reencoded_hex, has_control) AS (
        SELECT value, reject_controls, 1, '', 0
        FROM values_to_check
        WHERE value IS NOT NULL
        UNION ALL
        SELECT
            value,
            reject_controls,
            position + 1,
            reencoded_hex
                || hex(CAST(char(unicode(substr(value, position, 1))) AS BLOB)),
            CASE
                WHEN has_control = 1 OR (
                    reject_controls = 1
                    AND (
                        unicode(substr(value, position, 1)) < 32
                        OR unicode(substr(value, position, 1)) BETWEEN 127 AND 159
                    )
                ) THEN 1
                ELSE 0
            END
        FROM characters
        WHERE position <= length(value)
    )
    SELECT 1 FROM characters
    WHERE position = length(value) + 1
      AND (has_control = 1 OR reencoded_hex != hex(CAST(value AS BLOB)))
)
BEGIN
    SELECT RAISE(ABORT, 'cleanup operation text is not canonical');
END;

CREATE TRIGGER task_delivery_operation_transitions_no_replace
BEFORE INSERT ON task_delivery_operation_transitions
WHEN EXISTS (
    SELECT 1
    FROM task_delivery_operation_transitions t
    WHERE t.entity_kind = NEW.entity_kind
      AND t.entity_id = NEW.entity_id
      AND t.entity_version = NEW.entity_version
)
BEGIN
    SELECT RAISE(ABORT, 'delivery operation transitions are immutable');
END;

CREATE TRIGGER task_delivery_operation_transitions_match_current
BEFORE INSERT ON task_delivery_operation_transitions
WHEN NOT (
    (
        NEW.entity_kind = 'delivery_source'
        AND EXISTS (
            SELECT 1 FROM task_delivery_sources s
            WHERE s.task_id = NEW.entity_id
              AND s.version = NEW.entity_version
              AND s.state = NEW.to_state
              AND s.failure_code IS NEW.failure_code
              AND s.updated_at = NEW.transitioned_at
        )
    )
    OR (
        NEW.entity_kind = 'merge_operation'
        AND EXISTS (
            SELECT 1 FROM task_merge_operations m
            WHERE m.operation_id = NEW.entity_id
              AND m.version = NEW.entity_version
              AND m.state = NEW.to_state
              AND m.failure_code IS NEW.failure_code
              AND m.target_config_attributes_digest IS NEW.target_config_attributes_digest
              AND m.target_security_digest IS NEW.target_security_digest
              AND m.updated_at = NEW.transitioned_at
        )
    )
    OR (
        NEW.entity_kind = 'cleanup_operation'
        AND EXISTS (
            SELECT 1 FROM task_cleanup_operations c
            WHERE c.operation_id = NEW.entity_id
              AND c.version = NEW.entity_version
              AND c.state = NEW.to_state
              AND c.failure_code IS NEW.failure_code
              AND c.updated_at = NEW.transitioned_at
        )
    )
    OR (
        NEW.entity_kind = 'worktree_disposition'
        AND EXISTS (
            SELECT 1 FROM task_artifact_dispositions d
            WHERE d.task_id = NEW.entity_id
              AND d.worktree_version = NEW.entity_version
              AND d.worktree_state = NEW.to_state
              AND d.worktree_failure_code IS NEW.failure_code
              AND d.worktree_updated_at = NEW.transitioned_at
        )
    )
    OR (
        NEW.entity_kind = 'branch_disposition'
        AND EXISTS (
            SELECT 1 FROM task_artifact_dispositions d
            WHERE d.task_id = NEW.entity_id
              AND d.branch_version = NEW.entity_version
              AND d.branch_state = NEW.to_state
              AND d.branch_failure_code IS NEW.failure_code
              AND d.branch_updated_at = NEW.transitioned_at
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'delivery transition does not match its current row');
END;

CREATE TRIGGER task_delivery_operation_transitions_no_update
BEFORE UPDATE ON task_delivery_operation_transitions
BEGIN
    SELECT RAISE(ABORT, 'delivery operation transitions are immutable');
END;

CREATE TRIGGER task_delivery_operation_transitions_no_delete
BEFORE DELETE ON task_delivery_operation_transitions
BEGIN
    SELECT RAISE(ABORT, 'delivery operation transitions are immutable');
END;

CREATE TRIGGER task_delivery_sources_initial_on_insert
BEFORE INSERT ON task_delivery_sources
WHEN NEW.state != 'object_pending'
    OR NEW.version != 1
    OR NEW.created_at != NEW.updated_at
    OR NEW.failure_code IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'delivery source must start at ObjectPending version one');
END;

CREATE TRIGGER task_delivery_sources_ownership_on_insert
BEFORE INSERT ON task_delivery_sources
WHEN NOT EXISTS (
    SELECT 1
    FROM tasks t
    JOIN task_attempt_artifacts a
      ON a.task_id = t.id
     AND a.repository_id = t.repository_id
     AND a.attempt = t.attempt
    JOIN task_delivery_state d ON d.task_id = t.id
    JOIN task_review_evidence r
      ON r.task_id = t.id
     AND r.review_round = d.final_review_round
     AND r.verdict = d.final_verdict
    WHERE t.id = NEW.task_id
      AND t.repository_id = NEW.repository_id
      AND t.attempt = NEW.attempt
      AND t.status = 'completed'
      AND d.readiness = 'review_approved'
      AND d.final_verdict = 'approved'
      AND r.event_id = NEW.final_review_event_id
      AND r.review_round = NEW.final_review_round
      AND r.workspace_generation = NEW.workspace_generation
      AND r.digest_algorithm = 'workspace_fingerprint_v1'
      AND r.workspace_digest = NEW.workspace_fingerprint
      AND a.state = 'ready'
      AND a.base_commit = NEW.artifact_base_commit
      AND 'refs/heads/' || a.branch_name = NEW.artifact_source_branch
      AND a.worktree_path = NEW.artifact_worktree_path
)
OR NOT EXISTS (
    SELECT 1
    FROM task_merge_operations m
    JOIN task_delivery_command_receipts receipt
      ON receipt.client_request_id = NEW.origin_accept_receipt_id
     AND receipt.command_kind = 'accept_merge'
     AND receipt.operation_kind = 'merge_operation'
     AND receipt.operation_id = m.operation_id
     AND receipt.merge_operation_id = m.operation_id
     AND receipt.cleanup_operation_id IS NULL
     AND receipt.task_id = NEW.task_id
     AND receipt.repository_id = NEW.repository_id
     AND receipt.attempt = NEW.attempt
     AND receipt.accepted_operation_version = NEW.origin_accepted_version
     AND receipt.accepted_operation_state = 'accepted'
     AND receipt.response_discriminator = 'merge_accepted'
    JOIN task_delivery_operation_transitions transition
      ON transition.entity_kind = 'merge_operation'
     AND transition.entity_id = m.operation_id
     AND transition.entity_version = NEW.origin_accepted_version
     AND transition.from_state = 'preflight_ready'
     AND transition.to_state = 'accepted'
     AND transition.failure_code IS NULL
     AND transition.transitioned_at = receipt.created_at
    WHERE m.task_id = NEW.task_id
      AND m.operation_id = NEW.origin_accepted_operation_id
      AND m.repository_id = NEW.repository_id
      AND m.attempt = NEW.attempt
      AND m.state = 'accepted'
      AND m.version = NEW.origin_accepted_version
      AND m.accept_receipt_id = NEW.origin_accept_receipt_id
      AND m.evidence_algorithm = NEW.evidence_algorithm
      AND m.final_review_round = NEW.final_review_round
      AND m.final_review_event_id = NEW.final_review_event_id
      AND m.workspace_generation = NEW.workspace_generation
      AND m.workspace_fingerprint = NEW.workspace_fingerprint
      AND m.checks_digest = NEW.checks_digest
      AND m.coverage_digest = NEW.coverage_digest
      AND m.artifact_base_commit = NEW.artifact_base_commit
      AND m.artifact_source_branch = NEW.artifact_source_branch
      AND m.artifact_worktree_path = NEW.artifact_worktree_path
      AND m.common_git_identity_algorithm = NEW.common_git_identity_algorithm
      AND m.common_git_identity_digest = NEW.common_git_identity_digest
      AND m.worktree_admin_identity_algorithm = NEW.worktree_admin_identity_algorithm
      AND m.worktree_admin_identity_digest = NEW.worktree_admin_identity_digest
      AND m.fixed_lock_reason = NEW.fixed_lock_reason
      AND m.config_attributes_digest = NEW.config_attributes_digest
      AND m.candidate_tree_oid = NEW.candidate_tree_oid
)
BEGIN
    SELECT RAISE(ABORT, 'delivery source ownership is inconsistent');
END;

CREATE TRIGGER task_delivery_sources_no_replace
BEFORE INSERT ON task_delivery_sources
WHEN EXISTS (SELECT 1 FROM task_delivery_sources s WHERE s.task_id = NEW.task_id)
BEGIN
    SELECT RAISE(ABORT, 'delivery source current row cannot be replaced');
END;

CREATE TRIGGER task_delivery_sources_immutable_on_update
BEFORE UPDATE ON task_delivery_sources
WHEN NEW.task_id IS NOT OLD.task_id
    OR NEW.repository_id IS NOT OLD.repository_id
    OR NEW.attempt IS NOT OLD.attempt
    OR NEW.evidence_algorithm IS NOT OLD.evidence_algorithm
    OR NEW.final_review_round IS NOT OLD.final_review_round
    OR NEW.final_review_event_id IS NOT OLD.final_review_event_id
    OR NEW.workspace_generation IS NOT OLD.workspace_generation
    OR NEW.workspace_fingerprint IS NOT OLD.workspace_fingerprint
    OR NEW.checks_digest IS NOT OLD.checks_digest
    OR NEW.coverage_digest IS NOT OLD.coverage_digest
    OR NEW.artifact_base_commit IS NOT OLD.artifact_base_commit
    OR NEW.artifact_source_branch IS NOT OLD.artifact_source_branch
    OR NEW.artifact_worktree_path IS NOT OLD.artifact_worktree_path
    OR NEW.common_git_identity_algorithm IS NOT OLD.common_git_identity_algorithm
    OR NEW.common_git_identity_digest IS NOT OLD.common_git_identity_digest
    OR NEW.worktree_admin_identity_algorithm IS NOT OLD.worktree_admin_identity_algorithm
    OR NEW.worktree_admin_identity_digest IS NOT OLD.worktree_admin_identity_digest
    OR NEW.fixed_lock_reason IS NOT OLD.fixed_lock_reason
    OR NEW.config_attributes_digest IS NOT OLD.config_attributes_digest
    OR NEW.origin_accepted_operation_id IS NOT OLD.origin_accepted_operation_id
    OR NEW.origin_accept_receipt_id IS NOT OLD.origin_accept_receipt_id
    OR NEW.origin_accepted_version IS NOT OLD.origin_accepted_version
    OR NEW.candidate_tree_oid IS NOT OLD.candidate_tree_oid
    OR NEW.expected_parent_oid IS NOT OLD.expected_parent_oid
    OR NEW.author_name IS NOT OLD.author_name
    OR NEW.author_email IS NOT OLD.author_email
    OR NEW.committer_name IS NOT OLD.committer_name
    OR NEW.committer_email IS NOT OLD.committer_email
    OR NEW.author_date_bytes IS NOT OLD.author_date_bytes
    OR NEW.committer_date_bytes IS NOT OLD.committer_date_bytes
    OR NEW.commit_message_template_version IS NOT OLD.commit_message_template_version
    OR NEW.commit_message_bytes IS NOT OLD.commit_message_bytes
    OR NEW.created_at IS NOT OLD.created_at
    OR (
        OLD.expected_source_commit_oid IS NOT NULL
        AND NEW.expected_source_commit_oid IS NOT OLD.expected_source_commit_oid
    )
    OR (
        OLD.expected_source_commit_oid IS NULL
        AND NEW.expected_source_commit_oid IS NOT NULL
        AND NOT (OLD.state = 'object_pending' AND NEW.state = 'commit_pending')
    )
BEGIN
    SELECT RAISE(ABORT, 'delivery source provenance is immutable');
END;

CREATE TRIGGER task_delivery_sources_transition_on_update
BEFORE UPDATE ON task_delivery_sources
WHEN NEW.version != OLD.version + 1
    OR NOT (
        (OLD.state = 'object_pending' AND NEW.state IN (
            'object_pending', 'commit_pending', 'reconciliation_required'
        ))
        OR (OLD.state = 'commit_pending' AND NEW.state IN (
            'commit_pending', 'committed', 'reconciliation_required'
        ))
        OR (OLD.state = 'committed' AND NEW.state = 'reconciliation_required')
    )
BEGIN
    SELECT RAISE(ABORT, 'illegal delivery source transition');
END;

CREATE TRIGGER task_delivery_sources_merge_consistency_on_update
BEFORE UPDATE ON task_delivery_sources
WHEN NEW.state != 'committed'
    AND EXISTS (
        SELECT 1
        FROM task_merge_operations m
        WHERE m.delivery_source_task_id = NEW.task_id
          AND m.state IN ('merge_pending', 'merged', 'abort_pending')
    )
BEGIN
    SELECT RAISE(ABORT, 'delivery source conflicts with merge state');
END;

CREATE TRIGGER task_delivery_sources_no_delete
BEFORE DELETE ON task_delivery_sources
BEGIN
    SELECT RAISE(ABORT, 'delivery source current rows are retained');
END;

CREATE TRIGGER task_delivery_sources_journal_on_insert
AFTER INSERT ON task_delivery_sources
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, transitioned_at
    ) VALUES (
        'delivery_source', NEW.task_id, NEW.version, 'absent', NEW.state,
        NEW.failure_code, NEW.updated_at
    );
END;

CREATE TRIGGER task_delivery_sources_journal_on_update
AFTER UPDATE ON task_delivery_sources
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, transitioned_at
    ) VALUES (
        'delivery_source', NEW.task_id, NEW.version, OLD.state, NEW.state,
        NEW.failure_code, NEW.updated_at
    );
END;

CREATE TRIGGER task_merge_operations_initial_on_insert
BEFORE INSERT ON task_merge_operations
WHEN NEW.state != 'preflight_pending'
    OR NEW.version != 1
    OR NEW.created_at != NEW.updated_at
    OR NEW.failure_code IS NOT NULL
    OR NEW.accept_receipt_id IS NOT NULL
    OR NEW.delivery_source_task_id IS NOT NULL
    OR NEW.source_commit_oid IS NOT NULL
    OR NEW.merge_base_oid IS NOT NULL
    OR NEW.candidate_merge_tree_oid IS NOT NULL
    OR NEW.conflict_path_count IS NOT NULL
    OR NEW.merge_author_name IS NOT NULL
    OR NEW.merge_author_email IS NOT NULL
    OR NEW.merge_committer_name IS NOT NULL
    OR NEW.merge_committer_email IS NOT NULL
    OR NEW.merge_author_date_bytes IS NOT NULL
    OR NEW.merge_committer_date_bytes IS NOT NULL
    OR NEW.merge_message_template_version IS NOT NULL
    OR NEW.merge_message_bytes IS NOT NULL
    OR NEW.expected_merge_commit_oid IS NOT NULL
    OR NEW.abort_child_receipt_id IS NOT NULL
    OR NEW.merged_disposition_task_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'merge operation must start at PreflightPending version one');
END;

CREATE TRIGGER task_merge_operations_eligibility_on_insert
BEFORE INSERT ON task_merge_operations
WHEN NOT EXISTS (
    SELECT 1
    FROM tasks t
    JOIN task_attempt_artifacts a
      ON a.task_id = t.id
     AND a.repository_id = t.repository_id
     AND a.attempt = t.attempt
    JOIN task_delivery_state d ON d.task_id = t.id
    JOIN task_review_evidence r
      ON r.task_id = t.id
     AND r.review_round = d.final_review_round
     AND r.verdict = d.final_verdict
    WHERE t.id = NEW.task_id
      AND t.repository_id = NEW.repository_id
      AND t.attempt = NEW.attempt
      AND t.status = 'completed'
      AND d.readiness = 'review_approved'
      AND d.final_verdict = 'approved'
      AND r.event_id = NEW.final_review_event_id
      AND r.review_round = NEW.final_review_round
      AND r.workspace_generation = NEW.workspace_generation
      AND r.digest_algorithm = 'workspace_fingerprint_v1'
      AND r.workspace_digest = NEW.workspace_fingerprint
      AND a.state = 'ready'
      AND a.base_commit = NEW.artifact_base_commit
      AND 'refs/heads/' || a.branch_name = NEW.artifact_source_branch
      AND a.worktree_path = NEW.artifact_worktree_path
)
BEGIN
    SELECT RAISE(ABORT, 'merge operation eligibility is inconsistent');
END;

CREATE TRIGGER task_merge_operations_blocked_on_insert
BEFORE INSERT ON task_merge_operations
WHEN EXISTS (
    SELECT 1
    FROM task_merge_operations m
    WHERE m.task_id = NEW.task_id
      AND m.state IN ('merged', 'reconciliation_required')
)
OR EXISTS (
    SELECT 1
    FROM task_delivery_sources s
    WHERE s.task_id = NEW.task_id
      AND s.state = 'reconciliation_required'
)
OR EXISTS (
    SELECT 1
    FROM task_cleanup_operations c
    WHERE c.task_id = NEW.task_id
      AND c.state = 'reconciliation_required'
)
BEGIN
    SELECT RAISE(ABORT, 'delivery mutation is blocked by terminal ownership');
END;

CREATE TRIGGER task_merge_operations_no_replace
BEFORE INSERT ON task_merge_operations
WHEN EXISTS (
    SELECT 1 FROM task_merge_operations m WHERE m.operation_id = NEW.operation_id
)
BEGIN
    SELECT RAISE(ABORT, 'merge operation current row cannot be replaced');
END;

CREATE TRIGGER task_merge_operations_immutable_on_update
BEFORE UPDATE ON task_merge_operations
WHEN NEW.operation_id IS NOT OLD.operation_id
    OR NEW.task_id IS NOT OLD.task_id
    OR NEW.repository_id IS NOT OLD.repository_id
    OR NEW.attempt IS NOT OLD.attempt
    OR NEW.evidence_algorithm IS NOT OLD.evidence_algorithm
    OR NEW.final_review_round IS NOT OLD.final_review_round
    OR NEW.final_review_event_id IS NOT OLD.final_review_event_id
    OR NEW.workspace_generation IS NOT OLD.workspace_generation
    OR NEW.workspace_fingerprint IS NOT OLD.workspace_fingerprint
    OR NEW.checks_digest IS NOT OLD.checks_digest
    OR NEW.coverage_digest IS NOT OLD.coverage_digest
    OR NEW.artifact_base_commit IS NOT OLD.artifact_base_commit
    OR NEW.artifact_source_branch IS NOT OLD.artifact_source_branch
    OR NEW.artifact_worktree_path IS NOT OLD.artifact_worktree_path
    OR NEW.common_git_identity_algorithm IS NOT OLD.common_git_identity_algorithm
    OR NEW.common_git_identity_digest IS NOT OLD.common_git_identity_digest
    OR NEW.worktree_admin_identity_algorithm IS NOT OLD.worktree_admin_identity_algorithm
    OR NEW.worktree_admin_identity_digest IS NOT OLD.worktree_admin_identity_digest
    OR NEW.fixed_lock_reason IS NOT OLD.fixed_lock_reason
    OR (
        (
            NEW.candidate_tree_oid IS NOT OLD.candidate_tree_oid
            OR NEW.preflight_source_commit_oid IS NOT OLD.preflight_source_commit_oid
        )
        AND NOT (
            OLD.state = 'preflight_pending'
            AND NEW.state = 'preflight_pending'
            AND OLD.version = 1
            AND NEW.version = 2
            AND OLD.candidate_tree_oid IS NULL
            AND OLD.preflight_source_commit_oid IS NULL
            AND NEW.candidate_tree_oid IS NOT NULL
            AND NEW.preflight_source_commit_oid IS NOT NULL
        )
    )
    OR NEW.preflight_receipt_id IS NOT OLD.preflight_receipt_id
    OR NEW.target_branch IS NOT OLD.target_branch
    OR NEW.expected_target_head IS NOT OLD.expected_target_head
    OR NEW.config_attributes_digest IS NOT OLD.config_attributes_digest
    OR NEW.target_config_attributes_digest IS NOT OLD.target_config_attributes_digest
    OR NEW.target_security_digest IS NOT OLD.target_security_digest
    OR NEW.created_at IS NOT OLD.created_at
    OR (OLD.delivery_source_task_id IS NOT NULL AND NEW.delivery_source_task_id IS NOT OLD.delivery_source_task_id)
    OR (OLD.source_commit_oid IS NOT NULL AND NEW.source_commit_oid IS NOT OLD.source_commit_oid)
    OR (OLD.accept_receipt_id IS NOT NULL AND NEW.accept_receipt_id IS NOT OLD.accept_receipt_id)
    OR (OLD.merge_base_oid IS NOT NULL AND NEW.merge_base_oid IS NOT OLD.merge_base_oid)
    OR (OLD.candidate_merge_tree_oid IS NOT NULL AND NEW.candidate_merge_tree_oid IS NOT OLD.candidate_merge_tree_oid)
    OR (OLD.conflict_path_count IS NOT NULL AND NEW.conflict_path_count IS NOT OLD.conflict_path_count)
    OR (OLD.merge_author_name IS NOT NULL AND NEW.merge_author_name IS NOT OLD.merge_author_name)
    OR (OLD.merge_author_email IS NOT NULL AND NEW.merge_author_email IS NOT OLD.merge_author_email)
    OR (OLD.merge_committer_name IS NOT NULL AND NEW.merge_committer_name IS NOT OLD.merge_committer_name)
    OR (OLD.merge_committer_email IS NOT NULL AND NEW.merge_committer_email IS NOT OLD.merge_committer_email)
    OR (OLD.merge_author_date_bytes IS NOT NULL AND NEW.merge_author_date_bytes IS NOT OLD.merge_author_date_bytes)
    OR (OLD.merge_committer_date_bytes IS NOT NULL AND NEW.merge_committer_date_bytes IS NOT OLD.merge_committer_date_bytes)
    OR (OLD.merge_message_template_version IS NOT NULL AND NEW.merge_message_template_version IS NOT OLD.merge_message_template_version)
    OR (OLD.merge_message_bytes IS NOT NULL AND NEW.merge_message_bytes IS NOT OLD.merge_message_bytes)
    OR (OLD.expected_merge_commit_oid IS NOT NULL AND NEW.expected_merge_commit_oid IS NOT OLD.expected_merge_commit_oid)
    OR (OLD.abort_child_receipt_id IS NOT NULL AND NEW.abort_child_receipt_id IS NOT OLD.abort_child_receipt_id)
    OR (OLD.abort_merge_head_oid IS NOT NULL AND NEW.abort_merge_head_oid IS NOT OLD.abort_merge_head_oid)
    OR (OLD.abort_index_stages_digest IS NOT NULL AND NEW.abort_index_stages_digest IS NOT OLD.abort_index_stages_digest)
    OR (OLD.abort_worktree_digest IS NOT NULL AND NEW.abort_worktree_digest IS NOT OLD.abort_worktree_digest)
    OR (OLD.abort_merge_autostash_proof IS NOT NULL AND NEW.abort_merge_autostash_proof IS NOT OLD.abort_merge_autostash_proof)
    OR (OLD.merged_disposition_task_id IS NOT NULL AND NEW.merged_disposition_task_id IS NOT OLD.merged_disposition_task_id)
    OR (
        OLD.merge_base_oid IS NULL
        AND NEW.merge_base_oid IS NOT NULL
        AND NOT (
            OLD.state = 'preflight_pending'
            AND NEW.state IN ('preflight_ready', 'conflict')
        )
    )
    OR (
        OLD.candidate_merge_tree_oid IS NULL
        AND NEW.candidate_merge_tree_oid IS NOT NULL
        AND NOT (
            OLD.state = 'preflight_pending'
            AND NEW.state IN ('preflight_ready', 'conflict')
        )
    )
    OR (
        OLD.accept_receipt_id IS NULL
        AND NEW.accept_receipt_id IS NOT NULL
        AND NOT (OLD.state = 'preflight_ready' AND NEW.state = 'accepted')
    )
    OR (
        OLD.conflict_path_count IS NULL
        AND NEW.conflict_path_count IS NOT NULL
        AND NOT (
            (OLD.state = 'preflight_pending' AND NEW.state = 'conflict')
            OR (OLD.state = 'merge_pending' AND NEW.state = 'abort_pending')
        )
    )
    OR (
        OLD.merge_author_name IS NULL
        AND NEW.merge_author_name IS NOT NULL
        AND NOT (OLD.state = 'preflight_ready' AND NEW.state = 'accepted')
    )
    OR (
        OLD.delivery_source_task_id IS NULL
        AND NEW.delivery_source_task_id IS NOT NULL
        AND NOT (OLD.state = 'accepted' AND NEW.state IN ('merge_pending', 'failed'))
    )
    OR (
        OLD.expected_merge_commit_oid IS NULL
        AND NEW.expected_merge_commit_oid IS NOT NULL
        AND NOT (OLD.state = 'accepted' AND NEW.state = 'merge_pending')
    )
    OR (
        OLD.abort_child_receipt_id IS NULL
        AND NEW.abort_child_receipt_id IS NOT NULL
        AND NOT (OLD.state = 'merge_pending' AND NEW.state = 'abort_pending')
    )
    OR (
        OLD.merged_disposition_task_id IS NULL
        AND NEW.merged_disposition_task_id IS NOT NULL
        AND NOT (OLD.state = 'merge_pending' AND NEW.state = 'merged')
    )
BEGIN
    SELECT RAISE(ABORT, 'merge operation provenance is immutable');
END;

CREATE TRIGGER task_merge_operations_transition_on_update
BEFORE UPDATE ON task_merge_operations
WHEN NEW.version != OLD.version + 1
    OR NOT (
        (
            OLD.state = 'preflight_pending'
            AND NEW.state = 'preflight_pending'
            AND OLD.version = 1
            AND NEW.version = 2
            AND OLD.candidate_tree_oid IS NULL
            AND OLD.preflight_source_commit_oid IS NULL
            AND NEW.candidate_tree_oid IS NOT NULL
            AND NEW.preflight_source_commit_oid IS NOT NULL
        )
        OR (OLD.state = 'preflight_pending' AND NEW.state IN (
            'preflight_ready', 'conflict', 'rejected', 'stale',
            'reconciliation_required'
        ))
        OR (OLD.state = 'preflight_ready' AND NEW.state IN (
            'accepted', 'stale', 'superseded', 'reconciliation_required'
        ))
        OR (OLD.state = 'accepted' AND NEW.state IN (
            'merge_pending', 'failed', 'reconciliation_required'
        ))
        OR (OLD.state = 'merge_pending' AND NEW.state IN (
            'merged', 'abort_pending', 'failed', 'reconciliation_required'
        ))
        OR (OLD.state = 'abort_pending' AND NEW.state IN (
            'conflict', 'reconciliation_required'
        ))
    )
BEGIN
    SELECT RAISE(ABORT, 'illegal merge operation transition');
END;

CREATE TRIGGER task_merge_operations_source_consistency_on_update
BEFORE UPDATE ON task_merge_operations
WHEN NEW.delivery_source_task_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM task_delivery_sources s
        WHERE s.task_id = NEW.delivery_source_task_id
          AND s.repository_id = NEW.repository_id
          AND s.attempt = NEW.attempt
          AND s.state = 'committed'
          AND s.expected_source_commit_oid = NEW.source_commit_oid
          AND s.evidence_algorithm = NEW.evidence_algorithm
          AND s.final_review_round = NEW.final_review_round
          AND s.final_review_event_id = NEW.final_review_event_id
          AND s.workspace_generation = NEW.workspace_generation
          AND s.workspace_fingerprint = NEW.workspace_fingerprint
          AND s.checks_digest = NEW.checks_digest
          AND s.coverage_digest = NEW.coverage_digest
          AND s.candidate_tree_oid = NEW.candidate_tree_oid
    )
BEGIN
    SELECT RAISE(ABORT, 'merge operation source is not committed');
END;

CREATE TRIGGER task_merge_operations_source_reconciliation_on_update
BEFORE UPDATE ON task_merge_operations
WHEN NEW.state = 'reconciliation_required'
    AND EXISTS (
        SELECT 1
        FROM task_delivery_sources s
        WHERE s.task_id = NEW.task_id
          AND s.state IN ('object_pending', 'commit_pending')
    )
BEGIN
    SELECT RAISE(ABORT, 'pending delivery source must reconcile atomically');
END;

CREATE TRIGGER task_merge_operations_no_delete
BEFORE DELETE ON task_merge_operations
BEGIN
    SELECT RAISE(ABORT, 'merge operation current rows are retained');
END;

CREATE TRIGGER task_merge_operations_journal_on_insert
AFTER INSERT ON task_merge_operations
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, target_config_attributes_digest, target_security_digest,
        transitioned_at
    ) VALUES (
        'merge_operation', NEW.operation_id, NEW.version, 'absent', NEW.state,
        NEW.failure_code, NEW.target_config_attributes_digest, NEW.target_security_digest,
        NEW.updated_at
    );
END;

CREATE TRIGGER task_merge_operations_journal_on_update
AFTER UPDATE ON task_merge_operations
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, target_config_attributes_digest, target_security_digest,
        transitioned_at
    ) VALUES (
        'merge_operation', NEW.operation_id, NEW.version, OLD.state, NEW.state,
        NEW.failure_code, NEW.target_config_attributes_digest, NEW.target_security_digest,
        NEW.updated_at
    );
END;

CREATE TRIGGER task_merge_conflicts_text_canonical_on_insert
BEFORE INSERT ON task_merge_conflicts
WHEN EXISTS (
    WITH RECURSIVE characters(position, reencoded_hex) AS (
        VALUES (1, '')
        UNION ALL
        SELECT
            position + 1,
            reencoded_hex
                || hex(CAST(char(unicode(substr(NEW.path_value, position, 1))) AS BLOB))
        FROM characters
        WHERE position <= length(NEW.path_value)
    )
    SELECT 1 FROM characters
    WHERE position = length(NEW.path_value) + 1
      AND reencoded_hex != hex(CAST(NEW.path_value AS BLOB))
)
BEGIN
    SELECT RAISE(ABORT, 'merge conflict path text is not canonical');
END;

CREATE TRIGGER task_merge_conflicts_parent_on_insert
BEFORE INSERT ON task_merge_conflicts
WHEN NOT EXISTS (
    SELECT 1
    FROM task_merge_operations m
    WHERE m.operation_id = NEW.operation_id
      AND m.state IN ('abort_pending', 'conflict')
      AND m.conflict_path_count IS NOT NULL
      AND NEW.ordinal < m.conflict_path_count
)
BEGIN
    SELECT RAISE(ABORT, 'merge conflict requires a Conflict operation');
END;

CREATE TRIGGER task_merge_conflicts_bounds_on_insert
BEFORE INSERT ON task_merge_conflicts
WHEN (
        SELECT COUNT(*) FROM task_merge_conflicts c
        WHERE c.operation_id = NEW.operation_id
    ) >= 128
    OR COALESCE((
        SELECT SUM(length(CAST(c.path_value AS BLOB)))
        FROM task_merge_conflicts c
        WHERE c.operation_id = NEW.operation_id
    ), 0) + length(CAST(NEW.path_value AS BLOB)) > 65536
BEGIN
    SELECT RAISE(ABORT, 'merge conflict summary exceeds its bound');
END;

CREATE TRIGGER task_merge_conflicts_no_replace
BEFORE INSERT ON task_merge_conflicts
WHEN EXISTS (
    SELECT 1 FROM task_merge_conflicts c
    WHERE c.operation_id = NEW.operation_id AND c.ordinal = NEW.ordinal
)
BEGIN
    SELECT RAISE(ABORT, 'merge conflict rows are immutable');
END;

CREATE TRIGGER task_merge_conflicts_no_update
BEFORE UPDATE ON task_merge_conflicts
BEGIN
    SELECT RAISE(ABORT, 'merge conflict rows are immutable');
END;

CREATE TRIGGER task_merge_conflicts_no_delete
BEFORE DELETE ON task_merge_conflicts
BEGIN
    SELECT RAISE(ABORT, 'merge conflict rows are immutable');
END;

CREATE TRIGGER task_artifact_dispositions_initial_on_insert
BEFORE INSERT ON task_artifact_dispositions
WHEN NEW.worktree_state != 'retained_locked'
    OR NEW.worktree_version != 1
    OR NEW.worktree_failure_code IS NOT NULL
    OR NEW.worktree_cleanup_operation_id IS NOT NULL
    OR NEW.worktree_cleanup_operation_version IS NOT NULL
    OR NEW.worktree_cleanup_operation_state IS NOT NULL
    OR NEW.branch_state != 'retained'
    OR NEW.branch_version != 1
    OR NEW.branch_failure_code IS NOT NULL
    OR NEW.branch_cleanup_operation_id IS NOT NULL
    OR NEW.branch_cleanup_operation_version IS NOT NULL
    OR NEW.branch_cleanup_operation_state IS NOT NULL
    OR NEW.created_at != NEW.worktree_updated_at
    OR NEW.created_at != NEW.branch_updated_at
BEGIN
    SELECT RAISE(ABORT, 'artifact disposition must start at retained version one');
END;

CREATE TRIGGER task_artifact_dispositions_ownership_on_insert
BEFORE INSERT ON task_artifact_dispositions
WHEN NOT EXISTS (
    SELECT 1
    FROM task_merge_operations m
    JOIN task_delivery_sources s
      ON s.task_id = m.delivery_source_task_id
     AND s.expected_source_commit_oid = m.source_commit_oid
    WHERE m.operation_id = NEW.merged_operation_id
      AND m.merged_disposition_task_id = NEW.task_id
      AND m.task_id = NEW.task_id
      AND m.repository_id = NEW.repository_id
      AND m.attempt = NEW.attempt
      AND m.state = 'merged'
      AND s.state = 'committed'
      AND s.task_id = NEW.delivery_source_task_id
      AND s.expected_source_commit_oid = NEW.source_commit_oid
)
BEGIN
    SELECT RAISE(ABORT, 'artifact disposition ownership is inconsistent');
END;

CREATE TRIGGER task_artifact_dispositions_no_replace
BEFORE INSERT ON task_artifact_dispositions
WHEN EXISTS (
    SELECT 1 FROM task_artifact_dispositions d WHERE d.task_id = NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT, 'artifact disposition current row cannot be replaced');
END;

CREATE TRIGGER task_artifact_dispositions_immutable_on_update
BEFORE UPDATE ON task_artifact_dispositions
WHEN NEW.task_id IS NOT OLD.task_id
    OR NEW.repository_id IS NOT OLD.repository_id
    OR NEW.attempt IS NOT OLD.attempt
    OR NEW.merged_operation_id IS NOT OLD.merged_operation_id
    OR NEW.delivery_source_task_id IS NOT OLD.delivery_source_task_id
    OR NEW.source_commit_oid IS NOT OLD.source_commit_oid
    OR NEW.created_at IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'artifact disposition ownership is immutable');
END;

CREATE TRIGGER task_artifact_dispositions_transition_on_update
BEFORE UPDATE ON task_artifact_dispositions
WHEN NOT (
    (
        NEW.branch_state IS OLD.branch_state
        AND NEW.branch_version IS OLD.branch_version
        AND NEW.branch_failure_code IS OLD.branch_failure_code
        AND NEW.branch_updated_at IS OLD.branch_updated_at
        AND NEW.branch_cleanup_operation_id IS OLD.branch_cleanup_operation_id
        AND NEW.branch_cleanup_operation_version IS OLD.branch_cleanup_operation_version
        AND NEW.branch_cleanup_operation_state IS OLD.branch_cleanup_operation_state
        AND NEW.worktree_version = OLD.worktree_version + 1
        AND NEW.worktree_cleanup_operation_id IS NOT NULL
        AND NEW.worktree_cleanup_operation_version IS NOT NULL
        AND NEW.worktree_cleanup_operation_state IS NOT NULL
        AND (
            NEW.worktree_cleanup_operation_id IS NOT OLD.worktree_cleanup_operation_id
            OR NEW.worktree_cleanup_operation_version IS NOT OLD.worktree_cleanup_operation_version
            OR NEW.worktree_cleanup_operation_state IS NOT OLD.worktree_cleanup_operation_state
        )
        AND (
            (OLD.worktree_state = 'retained_locked' AND NEW.worktree_state IN (
                'retained_unlocked', 'reconciliation_required'
            ))
            OR (OLD.worktree_state = 'retained_unlocked' AND NEW.worktree_state IN (
                'removed', 'reconciliation_required'
            ))
            OR (OLD.worktree_state = 'removed' AND NEW.worktree_state = 'reconciliation_required')
        )
    )
    OR (
        NEW.worktree_state IS OLD.worktree_state
        AND NEW.worktree_version IS OLD.worktree_version
        AND NEW.worktree_failure_code IS OLD.worktree_failure_code
        AND NEW.worktree_updated_at IS OLD.worktree_updated_at
        AND NEW.worktree_cleanup_operation_id IS OLD.worktree_cleanup_operation_id
        AND NEW.worktree_cleanup_operation_version IS OLD.worktree_cleanup_operation_version
        AND NEW.worktree_cleanup_operation_state IS OLD.worktree_cleanup_operation_state
        AND NEW.worktree_state = 'removed'
        AND NEW.branch_version = OLD.branch_version + 1
        AND NEW.branch_cleanup_operation_id IS NOT NULL
        AND NEW.branch_cleanup_operation_version IS NOT NULL
        AND NEW.branch_cleanup_operation_state IS NOT NULL
        AND (
            NEW.branch_cleanup_operation_id IS NOT OLD.branch_cleanup_operation_id
            OR NEW.branch_cleanup_operation_version IS NOT OLD.branch_cleanup_operation_version
            OR NEW.branch_cleanup_operation_state IS NOT OLD.branch_cleanup_operation_state
        )
        AND (
            (OLD.branch_state = 'retained' AND NEW.branch_state IN (
                'deleted', 'reconciliation_required'
            ))
            OR (OLD.branch_state = 'deleted' AND NEW.branch_state = 'reconciliation_required')
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'illegal artifact disposition transition');
END;

CREATE TRIGGER task_artifact_dispositions_no_delete
BEFORE DELETE ON task_artifact_dispositions
BEGIN
    SELECT RAISE(ABORT, 'artifact disposition current rows are retained');
END;

CREATE TRIGGER task_artifact_dispositions_worktree_journal_on_insert
AFTER INSERT ON task_artifact_dispositions
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, transitioned_at
    ) VALUES (
        'worktree_disposition', NEW.task_id, NEW.worktree_version,
        'absent', NEW.worktree_state, NEW.worktree_failure_code,
        NEW.worktree_updated_at
    );
END;

CREATE TRIGGER task_artifact_dispositions_branch_journal_on_insert
AFTER INSERT ON task_artifact_dispositions
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, transitioned_at
    ) VALUES (
        'branch_disposition', NEW.task_id, NEW.branch_version,
        'absent', NEW.branch_state, NEW.branch_failure_code,
        NEW.branch_updated_at
    );
END;

CREATE TRIGGER task_artifact_dispositions_worktree_journal_on_update
AFTER UPDATE ON task_artifact_dispositions
WHEN NEW.worktree_version != OLD.worktree_version
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, transitioned_at
    ) VALUES (
        'worktree_disposition', NEW.task_id, NEW.worktree_version,
        OLD.worktree_state, NEW.worktree_state, NEW.worktree_failure_code,
        NEW.worktree_updated_at
    );
END;

CREATE TRIGGER task_artifact_dispositions_branch_journal_on_update
AFTER UPDATE ON task_artifact_dispositions
WHEN NEW.branch_version != OLD.branch_version
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, transitioned_at
    ) VALUES (
        'branch_disposition', NEW.task_id, NEW.branch_version,
        OLD.branch_state, NEW.branch_state, NEW.branch_failure_code,
        NEW.branch_updated_at
    );
END;

CREATE TRIGGER task_cleanup_operations_initial_on_insert
BEFORE INSERT ON task_cleanup_operations
WHEN NEW.version != 1
    OR NEW.created_at != NEW.updated_at
    OR NEW.failure_code IS NOT NULL
    OR NOT (
        (
            NEW.kind = 'remove_worktree'
            AND NEW.state IN ('unlock_pending', 'remove_pending')
            AND NEW.origin_target_head IS NULL
        )
        OR (
            NEW.kind = 'delete_branch'
            AND NEW.state = 'delete_pending'
            AND NEW.origin_target_head IS NEW.expected_target_head
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'cleanup operation has an invalid initial state');
END;

CREATE TRIGGER task_cleanup_operations_ownership_on_insert
BEFORE INSERT ON task_cleanup_operations
WHEN NOT EXISTS (
    SELECT 1
    FROM task_artifact_dispositions d
    JOIN task_delivery_sources s
      ON s.task_id = d.delivery_source_task_id
     AND s.expected_source_commit_oid = d.source_commit_oid
    JOIN task_merge_operations m
      ON m.operation_id = d.merged_operation_id
     AND m.merged_disposition_task_id = d.task_id
    WHERE d.task_id = NEW.disposition_task_id
      AND d.task_id = NEW.task_id
      AND d.repository_id = NEW.repository_id
      AND d.attempt = NEW.attempt
      AND s.state = 'committed'
      AND m.state = 'merged'
      AND s.artifact_worktree_path = NEW.expected_worktree_path
      AND s.worktree_admin_identity_algorithm = NEW.expected_admin_identity_algorithm
      AND s.worktree_admin_identity_digest = NEW.expected_admin_identity_digest
      AND s.common_git_identity_algorithm = NEW.expected_common_git_identity_algorithm
      AND s.common_git_identity_digest = NEW.expected_common_git_identity_digest
      AND s.artifact_source_branch = NEW.expected_source_ref
      AND s.expected_source_commit_oid = NEW.expected_source_oid
      AND (
          (
              NEW.kind = 'remove_worktree'
              AND d.branch_state = 'retained'
              AND d.worktree_version = NEW.expected_disposition_version
              AND (
                  (NEW.state = 'unlock_pending' AND d.worktree_state = 'retained_locked')
                  OR (NEW.state = 'remove_pending' AND d.worktree_state = 'retained_unlocked')
              )
          )
          OR (
              NEW.kind = 'delete_branch'
              AND d.worktree_state = 'removed'
              AND d.branch_state = 'retained'
              AND d.branch_version = NEW.expected_disposition_version
              AND NEW.state = 'delete_pending'
              AND NEW.expected_target_ref = m.target_branch
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'cleanup operation ownership is inconsistent');
END;

CREATE TRIGGER task_cleanup_operations_no_replace
BEFORE INSERT ON task_cleanup_operations
WHEN EXISTS (
    SELECT 1 FROM task_cleanup_operations c WHERE c.operation_id = NEW.operation_id
)
BEGIN
    SELECT RAISE(ABORT, 'cleanup operation current row cannot be replaced');
END;

CREATE TRIGGER task_cleanup_operations_immutable_on_update
BEFORE UPDATE ON task_cleanup_operations
WHEN NEW.operation_id IS NOT OLD.operation_id
    OR NEW.task_id IS NOT OLD.task_id
    OR NEW.repository_id IS NOT OLD.repository_id
    OR NEW.attempt IS NOT OLD.attempt
    OR NEW.kind IS NOT OLD.kind
    OR NEW.origin_receipt_id IS NOT OLD.origin_receipt_id
    OR NEW.disposition_task_id IS NOT OLD.disposition_task_id
    OR NEW.expected_worktree_path IS NOT OLD.expected_worktree_path
    OR NEW.expected_admin_identity_algorithm IS NOT OLD.expected_admin_identity_algorithm
    OR NEW.expected_admin_identity_digest IS NOT OLD.expected_admin_identity_digest
    OR NEW.expected_common_git_identity_algorithm IS NOT OLD.expected_common_git_identity_algorithm
    OR NEW.expected_common_git_identity_digest IS NOT OLD.expected_common_git_identity_digest
    OR NEW.expected_source_ref IS NOT OLD.expected_source_ref
    OR NEW.expected_source_oid IS NOT OLD.expected_source_oid
    OR NEW.expected_target_ref IS NOT OLD.expected_target_ref
    OR NEW.origin_target_head IS NOT OLD.origin_target_head
    OR NEW.created_at IS NOT OLD.created_at
    OR NEW.expected_disposition_version < OLD.expected_disposition_version
    OR NEW.expected_disposition_version > OLD.expected_disposition_version + 1
    OR (
        NEW.expected_target_head IS NOT OLD.expected_target_head
        AND NOT (
            OLD.kind = 'delete_branch'
            AND OLD.state = 'delete_pending'
            AND NEW.state = 'delete_pending'
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'cleanup operation provenance is immutable');
END;

CREATE TRIGGER task_cleanup_operations_transition_on_update
BEFORE UPDATE ON task_cleanup_operations
WHEN NEW.version != OLD.version + 1
    OR NOT (
        (
            OLD.kind = 'remove_worktree'
            AND NEW.kind = 'remove_worktree'
            AND (
                (
                    OLD.state = 'unlock_pending'
                    AND NEW.state = 'unlocked_pending_remove'
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version + 1
                )
                OR (
                    OLD.state = 'unlock_pending'
                    AND NEW.state = 'failed'
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version
                    AND NEW.failure_code = 'COMMAND_TIMED_OUT'
                )
                OR (
                    OLD.state = 'unlock_pending'
                    AND NEW.state = 'reconciliation_required'
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version + 1
                )
                OR (
                    OLD.state = 'unlocked_pending_remove'
                    AND NEW.state = 'remove_pending'
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version
                )
                OR (
                    OLD.state = 'unlocked_pending_remove'
                    AND NEW.state = 'reconciliation_required'
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version + 1
                )
                OR (
                    OLD.state = 'remove_pending'
                    AND NEW.state IN ('completed', 'reconciliation_required')
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version + 1
                )
                OR (
                    OLD.state = 'remove_pending'
                    AND NEW.state = 'failed'
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version
                    AND NEW.failure_code IN ('TARGET_WORKTREE_DIRTY', 'COMMAND_TIMED_OUT')
                )
            )
        )
        OR (
            OLD.kind = 'delete_branch'
            AND NEW.kind = 'delete_branch'
            AND OLD.state = 'delete_pending'
            AND (
                (
                    NEW.state = 'delete_pending'
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version
                    AND NEW.expected_target_head IS NOT OLD.expected_target_head
                )
                OR (
                    NEW.state IN ('completed', 'reconciliation_required')
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version + 1
                    AND NEW.expected_target_head IS OLD.expected_target_head
                )
                OR (
                    NEW.state = 'failed'
                    AND NEW.expected_disposition_version = OLD.expected_disposition_version
                    AND NEW.expected_target_head IS OLD.expected_target_head
                    AND NEW.failure_code IN (
                        'SOURCE_BRANCH_NOT_MERGED', 'COMMAND_TIMED_OUT'
                    )
                )
            )
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'illegal cleanup operation transition');
END;

CREATE TRIGGER task_cleanup_operations_disposition_on_update
BEFORE UPDATE ON task_cleanup_operations
WHEN NOT EXISTS (
    SELECT 1
    FROM task_artifact_dispositions d
    WHERE d.task_id = NEW.disposition_task_id
      AND d.task_id = NEW.task_id
      AND d.repository_id = NEW.repository_id
      AND d.attempt = NEW.attempt
      AND (
          (
              NEW.kind = 'remove_worktree'
              AND d.branch_state = 'retained'
              AND d.worktree_version = NEW.expected_disposition_version
              AND (
                  (NEW.state = 'unlock_pending' AND d.worktree_state = 'retained_locked')
                  OR (NEW.state IN ('unlocked_pending_remove', 'remove_pending') AND d.worktree_state = 'retained_unlocked')
                  OR (NEW.state = 'completed' AND d.worktree_state = 'removed')
                  OR (NEW.state = 'failed' AND d.worktree_state IN ('retained_locked', 'retained_unlocked'))
                  OR (NEW.state = 'reconciliation_required' AND d.worktree_state = 'reconciliation_required')
              )
          )
          OR (
              NEW.kind = 'delete_branch'
              AND d.worktree_state = 'removed'
              AND d.branch_version = NEW.expected_disposition_version
              AND (
                  (NEW.state IN ('delete_pending', 'failed') AND d.branch_state = 'retained')
                  OR (NEW.state = 'completed' AND d.branch_state = 'deleted')
                  OR (NEW.state = 'reconciliation_required' AND d.branch_state = 'reconciliation_required')
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'cleanup operation does not match current disposition');
END;

CREATE TRIGGER task_cleanup_operations_no_delete
BEFORE DELETE ON task_cleanup_operations
BEGIN
    SELECT RAISE(ABORT, 'cleanup operation current rows are retained');
END;

CREATE TRIGGER task_cleanup_target_head_observations_match_current
BEFORE INSERT ON task_cleanup_target_head_observations
WHEN NOT EXISTS (
    SELECT 1 FROM task_cleanup_operations cleanup
    WHERE cleanup.operation_id = NEW.cleanup_operation_id
      AND cleanup.kind = 'delete_branch'
      AND cleanup.version = NEW.operation_version
      AND cleanup.expected_target_head = NEW.target_head
      AND cleanup.updated_at = NEW.observed_at
)
BEGIN
    SELECT RAISE(ABORT, 'cleanup target head observation does not match current operation');
END;

CREATE TRIGGER task_cleanup_target_head_observations_no_replace
BEFORE INSERT ON task_cleanup_target_head_observations
WHEN EXISTS (
    SELECT 1 FROM task_cleanup_target_head_observations observation
    WHERE observation.cleanup_operation_id = NEW.cleanup_operation_id
      AND observation.operation_version = NEW.operation_version
)
BEGIN
    SELECT RAISE(ABORT, 'cleanup target head observations are immutable');
END;

CREATE TRIGGER task_cleanup_target_head_observations_no_update
BEFORE UPDATE ON task_cleanup_target_head_observations
BEGIN
    SELECT RAISE(ABORT, 'cleanup target head observations are immutable');
END;

CREATE TRIGGER task_cleanup_target_head_observations_no_delete
BEFORE DELETE ON task_cleanup_target_head_observations
BEGIN
    SELECT RAISE(ABORT, 'cleanup target head observations are immutable');
END;

CREATE TRIGGER task_cleanup_operations_journal_on_insert
AFTER INSERT ON task_cleanup_operations
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, transitioned_at
    ) VALUES (
        'cleanup_operation', NEW.operation_id, NEW.version, 'absent', NEW.state,
        NEW.failure_code, NEW.updated_at
    );
    INSERT INTO task_cleanup_target_head_observations (
        cleanup_operation_id, operation_version, target_head, observed_at
    )
    SELECT NEW.operation_id, NEW.version, NEW.expected_target_head, NEW.updated_at
    WHERE NEW.kind = 'delete_branch';
END;

CREATE TRIGGER task_cleanup_operations_journal_on_update
AFTER UPDATE ON task_cleanup_operations
BEGIN
    INSERT INTO task_delivery_operation_transitions (
        entity_kind, entity_id, entity_version, from_state, to_state,
        failure_code, transitioned_at
    ) VALUES (
        'cleanup_operation', NEW.operation_id, NEW.version, OLD.state, NEW.state,
        NEW.failure_code, NEW.updated_at
    );
    INSERT INTO task_cleanup_target_head_observations (
        cleanup_operation_id, operation_version, target_head, observed_at
    )
    SELECT NEW.operation_id, NEW.version, NEW.expected_target_head, NEW.updated_at
    WHERE NEW.kind = 'delete_branch';
END;

CREATE TRIGGER task_delivery_command_receipts_match_operation_on_insert
BEFORE INSERT ON task_delivery_command_receipts
WHEN NOT (
    (
        NEW.command_kind = 'preflight'
        AND EXISTS (
            SELECT 1 FROM task_merge_operations m
            WHERE m.operation_id = NEW.merge_operation_id
              AND m.preflight_receipt_id = NEW.client_request_id
              AND m.task_id = NEW.task_id
              AND m.repository_id = NEW.repository_id
              AND m.attempt = NEW.attempt
              AND m.version = NEW.accepted_operation_version
              AND m.state = NEW.accepted_operation_state
        )
    )
    OR (
        NEW.command_kind = 'accept_merge'
        AND EXISTS (
            SELECT 1 FROM task_merge_operations m
            WHERE m.operation_id = NEW.merge_operation_id
              AND m.accept_receipt_id = NEW.client_request_id
              AND m.task_id = NEW.task_id
              AND m.repository_id = NEW.repository_id
              AND m.attempt = NEW.attempt
              AND m.version = NEW.accepted_operation_version
              AND m.state = NEW.accepted_operation_state
        )
    )
    OR (
        NEW.command_kind IN ('remove_worktree', 'delete_branch')
        AND EXISTS (
            SELECT 1 FROM task_cleanup_operations c
            JOIN task_artifact_dispositions d
              ON d.task_id = c.disposition_task_id
            WHERE c.operation_id = NEW.cleanup_operation_id
              AND c.origin_receipt_id = NEW.client_request_id
              AND c.task_id = NEW.task_id
              AND c.repository_id = NEW.repository_id
              AND c.attempt = NEW.attempt
              AND c.version = NEW.accepted_operation_version
              AND c.state = NEW.accepted_operation_state
              AND d.merged_operation_id = NEW.cleanup_merged_operation_id
              AND (
                  (NEW.command_kind = 'remove_worktree' AND c.kind = 'remove_worktree')
                  OR (NEW.command_kind = 'delete_branch' AND c.kind = 'delete_branch')
              )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'command receipt does not match accepted operation');
END;

CREATE TRIGGER task_delivery_command_receipts_no_replace
BEFORE INSERT ON task_delivery_command_receipts
WHEN EXISTS (
    SELECT 1 FROM task_delivery_command_receipts r
    WHERE r.client_request_id = NEW.client_request_id
       OR (
           r.command_kind = NEW.command_kind
           AND r.operation_id = NEW.operation_id
       )
)
BEGIN
    SELECT RAISE(ABORT, 'delivery command receipts are immutable');
END;

CREATE TRIGGER task_delivery_command_receipts_no_update
BEFORE UPDATE ON task_delivery_command_receipts
BEGIN
    SELECT RAISE(ABORT, 'delivery command receipts are immutable');
END;

CREATE TRIGGER task_delivery_command_receipts_no_delete
BEFORE DELETE ON task_delivery_command_receipts
BEGIN
    SELECT RAISE(ABORT, 'delivery command receipts are immutable');
END;
