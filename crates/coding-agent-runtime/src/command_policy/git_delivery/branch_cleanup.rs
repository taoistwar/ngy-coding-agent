use super::*;

/// Task 17's branch-cleanup command vocabulary. Every builder consumes the
/// one opaque binding created from an authenticated registered checkout;
/// callers cannot choose a repository, ref, persisted CAS value, argv, or
/// stdin payload at the mutation boundary.
impl ValidatedCommand {
    pub(crate) fn validate_delivery_branch_cleanup_binding(
        authority: &DeliveryGitTargetMutationBinding,
        source_ref: &str,
        target_ref: &str,
        expected_source: &DeliveryCommitOid,
        expected_target: &DeliveryCommitOid,
    ) -> Result<(), CommandPolicyError> {
        authority.revalidate_for_executable(&authority.binding.git)?;
        require_delivery_source_ref(source_ref)?;
        require_delivery_target_ref(target_ref)?;
        if source_ref == target_ref {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        authority.require_object_id(expected_source.as_str())?;
        authority.require_object_id(expected_target.as_str())
    }

    pub(crate) fn delivery_branch_cleanup_source_ref_symbolic(
        binding: &DeliveryGitBranchCleanupBinding,
    ) -> Result<Self, CommandPolicyError> {
        validate_delivery_branch_cleanup_binding(binding)?;
        binding.authority_for_policy().command(
            [
                "symbolic-ref",
                "--quiet",
                "--no-recurse",
                "--",
                binding.source_ref_for_policy(),
            ],
            None,
            None,
        )
    }

    pub(crate) fn delivery_branch_cleanup_target_ref_symbolic(
        binding: &DeliveryGitBranchCleanupBinding,
    ) -> Result<Self, CommandPolicyError> {
        validate_delivery_branch_cleanup_binding(binding)?;
        binding.authority_for_policy().command(
            [
                "symbolic-ref",
                "--quiet",
                "--no-recurse",
                "--",
                binding.target_ref_for_policy(),
            ],
            None,
            None,
        )
    }

    pub(crate) fn delivery_branch_cleanup_resolve_source_ref(
        binding: &DeliveryGitBranchCleanupBinding,
    ) -> Result<Self, CommandPolicyError> {
        validate_delivery_branch_cleanup_binding(binding)?;
        binding.authority_for_policy().command(
            [
                "rev-parse",
                "--verify",
                "--quiet",
                "--end-of-options",
                binding.source_ref_for_policy(),
            ],
            None,
            None,
        )
    }

    pub(crate) fn delivery_branch_cleanup_resolve_target_ref(
        binding: &DeliveryGitBranchCleanupBinding,
    ) -> Result<Self, CommandPolicyError> {
        validate_delivery_branch_cleanup_binding(binding)?;
        binding.authority_for_policy().command(
            [
                "rev-parse",
                "--verify",
                "--quiet",
                "--end-of-options",
                binding.target_ref_for_policy(),
            ],
            None,
            None,
        )
    }

    pub(crate) fn delivery_branch_cleanup_expected_source_commit(
        binding: &DeliveryGitBranchCleanupBinding,
    ) -> Result<Self, CommandPolicyError> {
        branch_cleanup_cat_file_commit(binding, binding.expected_source_for_policy())
    }

    pub(crate) fn delivery_branch_cleanup_expected_target_commit(
        binding: &DeliveryGitBranchCleanupBinding,
    ) -> Result<Self, CommandPolicyError> {
        branch_cleanup_cat_file_commit(binding, binding.expected_target_for_policy())
    }

    pub(crate) fn delivery_branch_cleanup_fresh_target_commit(
        binding: &DeliveryGitBranchCleanupBinding,
        fresh_target: &DeliveryCommitOid,
    ) -> Result<Self, CommandPolicyError> {
        branch_cleanup_cat_file_commit(binding, fresh_target)
    }

    pub(crate) fn delivery_branch_cleanup_source_is_ancestor(
        binding: &DeliveryGitBranchCleanupBinding,
        fresh_target: &DeliveryCommitOid,
    ) -> Result<Self, CommandPolicyError> {
        validate_delivery_branch_cleanup_binding(binding)?;
        binding
            .authority_for_policy()
            .require_object_id(fresh_target.as_str())?;
        binding.authority_for_policy().command(
            [
                "merge-base",
                "--is-ancestor",
                binding.expected_source_for_policy().as_str(),
                fresh_target.as_str(),
            ],
            None,
            None,
        )
    }

    pub(crate) fn delivery_branch_cleanup_target_is_ancestor(
        binding: &DeliveryGitBranchCleanupBinding,
        fresh_target: &DeliveryCommitOid,
    ) -> Result<Self, CommandPolicyError> {
        validate_delivery_branch_cleanup_binding(binding)?;
        binding
            .authority_for_policy()
            .require_object_id(fresh_target.as_str())?;
        binding.authority_for_policy().command(
            [
                "merge-base",
                "--is-ancestor",
                binding.expected_target_for_policy().as_str(),
                fresh_target.as_str(),
            ],
            None,
            None,
        )
    }

    pub(crate) fn delivery_branch_cleanup_worktree_list(
        binding: &DeliveryGitBranchCleanupBinding,
    ) -> Result<Self, CommandPolicyError> {
        validate_delivery_branch_cleanup_binding(binding)?;
        binding.authority_for_policy().command(
            ["worktree", "list", "--porcelain", "-z"],
            None,
            None,
        )
    }

    pub(crate) fn delivery_branch_cleanup_delete_source(
        binding: &DeliveryGitBranchCleanupBinding,
    ) -> Result<Self, CommandPolicyError> {
        validate_delivery_branch_cleanup_binding(binding)?;
        let exact_input =
            ExactChildInput::try_new(delivery_branch_cleanup_transaction_input(binding))
                .map_err(|_| CommandPolicyError::InvalidGitBinding)?;
        binding.authority_for_policy().command(
            ["update-ref", "--no-deref", "--stdin"],
            Some(exact_input),
            None,
        )
    }
}

fn validate_delivery_branch_cleanup_binding(
    binding: &DeliveryGitBranchCleanupBinding,
) -> Result<(), CommandPolicyError> {
    ValidatedCommand::validate_delivery_branch_cleanup_binding(
        binding.authority_for_policy(),
        binding.source_ref_for_policy(),
        binding.target_ref_for_policy(),
        binding.expected_source_for_policy(),
        binding.expected_target_for_policy(),
    )
}

fn branch_cleanup_cat_file_commit(
    binding: &DeliveryGitBranchCleanupBinding,
    object: &DeliveryCommitOid,
) -> Result<ValidatedCommand, CommandPolicyError> {
    validate_delivery_branch_cleanup_binding(binding)?;
    binding
        .authority_for_policy()
        .require_object_id(object.as_str())?;
    let exact_input = ExactChildInput::try_new(cat_file_batch_input(object.as_str()))
        .map_err(|_| CommandPolicyError::InvalidGitBinding)?;
    binding
        .authority_for_policy()
        .command(["cat-file", "--batch"], Some(exact_input), None)
}

fn require_delivery_source_ref(source_ref: &str) -> Result<(), CommandPolicyError> {
    let source_branch = source_ref
        .strip_prefix("refs/heads/")
        .ok_or(CommandPolicyError::InvalidGitBinding)?;
    let expected = DeliveryGitSourceRef::try_new(source_branch)?;
    if expected.as_str() == source_ref {
        Ok(())
    } else {
        Err(CommandPolicyError::InvalidGitBinding)
    }
}

fn require_delivery_target_ref(target_ref: &str) -> Result<(), CommandPolicyError> {
    let target_branch = target_ref
        .strip_prefix("refs/heads/")
        .ok_or(CommandPolicyError::InvalidGitBinding)?;
    if is_safe_delivery_target_branch(target_branch) {
        Ok(())
    } else {
        Err(CommandPolicyError::InvalidGitBinding)
    }
}

fn is_safe_delivery_target_branch(value: &str) -> bool {
    const MAX_TARGET_BRANCH_BYTES: usize = 255;
    if value.is_empty()
        || value.len() > MAX_TARGET_BRANCH_BYTES
        || value == "@"
        || matches!(value.as_bytes().first(), Some(b'/' | b'-'))
        || matches!(value.as_bytes().last(), Some(b'/' | b'.'))
        || value.contains("..")
        || value.contains("//")
        || value.contains("@{")
        || value.bytes().any(|byte| {
            byte.is_ascii_control()
                || byte.is_ascii_whitespace()
                || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        })
    {
        return false;
    }
    value.split('/').all(|component| {
        !component.is_empty()
            && !component.starts_with('.')
            && !component.ends_with('.')
            && !component.ends_with(".lock")
    })
}

pub(super) fn delivery_branch_cleanup_transaction_input(
    binding: &DeliveryGitBranchCleanupBinding,
) -> Vec<u8> {
    format!(
        "start\nverify {} {}\ndelete {} {}\nprepare\ncommit\n",
        binding.target_ref_for_policy(),
        binding.expected_target_for_policy().as_str(),
        binding.source_ref_for_policy(),
        binding.expected_source_for_policy().as_str(),
    )
    .into_bytes()
}
