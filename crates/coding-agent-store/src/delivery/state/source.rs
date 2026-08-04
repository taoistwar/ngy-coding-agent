wire_enum!(DeliverySourceState {
    ObjectPending => "object_pending",
    CommitPending => "commit_pending",
    Committed => "committed",
    ReconciliationRequired => "reconciliation_required",
});

impl DeliverySourceState {
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::ObjectPending,
                Self::CommitPending | Self::ReconciliationRequired
            ) | (
                Self::CommitPending,
                Self::Committed | Self::ReconciliationRequired
            ) | (Self::Committed, Self::ReconciliationRequired)
        )
    }

    pub const fn is_side_effect_active(self) -> bool {
        matches!(self, Self::ObjectPending | Self::CommitPending)
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Committed)
    }

    pub const fn is_reconciliation(self) -> bool {
        matches!(self, Self::ReconciliationRequired)
    }
}
