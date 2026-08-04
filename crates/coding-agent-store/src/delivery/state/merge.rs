use super::super::DeliveryError;
use super::source::DeliverySourceState;

wire_enum!(MergeOperationState {
    PreflightPending => "preflight_pending",
    PreflightReady => "preflight_ready",
    Accepted => "accepted",
    MergePending => "merge_pending",
    Merged => "merged",
    AbortPending => "abort_pending",
    Conflict => "conflict",
    Rejected => "rejected",
    Stale => "stale",
    Superseded => "superseded",
    Failed => "failed",
    ReconciliationRequired => "reconciliation_required",
});

impl MergeOperationState {
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::PreflightPending,
                Self::PreflightReady
                    | Self::Conflict
                    | Self::Rejected
                    | Self::Stale
                    | Self::ReconciliationRequired
            ) | (
                Self::PreflightReady,
                Self::Accepted | Self::Stale | Self::Superseded | Self::ReconciliationRequired
            ) | (
                Self::Accepted,
                Self::MergePending | Self::Failed | Self::ReconciliationRequired
            ) | (
                Self::MergePending,
                Self::Merged | Self::AbortPending | Self::Failed | Self::ReconciliationRequired
            ) | (
                Self::AbortPending,
                Self::Conflict | Self::ReconciliationRequired
            )
        )
    }

    pub const fn is_open(self) -> bool {
        matches!(self, Self::PreflightPending | Self::PreflightReady)
    }

    pub const fn is_side_effect_active(self) -> bool {
        matches!(
            self,
            Self::Accepted | Self::MergePending | Self::AbortPending
        )
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Merged
                | Self::Conflict
                | Self::Rejected
                | Self::Stale
                | Self::Superseded
                | Self::Failed
        )
    }

    pub const fn is_reconciliation(self) -> bool {
        matches!(self, Self::ReconciliationRequired)
    }
}

pub fn validate_merge_source_state(
    merge: MergeOperationState,
    source: Option<DeliverySourceState>,
) -> Result<(), DeliveryError> {
    use DeliverySourceState::{
        CommitPending, Committed, ObjectPending, ReconciliationRequired as SourceReconciliation,
    };
    use MergeOperationState::{
        AbortPending, Accepted, Failed, MergePending, Merged,
        ReconciliationRequired as MergeReconciliation,
    };

    let valid = match (merge, source) {
        (MergeReconciliation, None | Some(Committed | SourceReconciliation)) => true,
        (MergeReconciliation, Some(ObjectPending | CommitPending)) => false,
        (_, Some(SourceReconciliation)) => false,
        (MergePending | Merged | AbortPending | Failed, Some(Committed)) => true,
        (MergePending | Merged | AbortPending | Failed, _) => false,
        (Accepted, None | Some(ObjectPending | CommitPending | Committed)) => true,
        (_, Some(ObjectPending | CommitPending)) => false,
        (_, None | Some(Committed)) => true,
    };
    if valid {
        Ok(())
    } else {
        Err(DeliveryError::InvalidStateCombination)
    }
}
