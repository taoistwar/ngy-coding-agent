use coding_agent_domain::TaskId;

use super::{
    DeliveryAllowedAction, DeliveryEligibility, DeliveryEligibilityReason, DeliveryTaskProjection,
    dto::DeliveryTaskProjectionContext,
};
#[cfg(test)]
use super::{DeliveryTargetObservation, DeliveryTargetUnavailableReason};

pub(crate) enum DeliveryProjectionDecision {
    Eligible(Vec<DeliveryAllowedAction>),
    Ineligible(Vec<DeliveryEligibilityReason>),
    Unavailable(Vec<DeliveryEligibilityReason>),
}

#[cfg(test)]
pub(crate) fn project_delivery_task(
    task_id: TaskId,
    decision: DeliveryProjectionDecision,
) -> DeliveryTaskProjection {
    project_delivery_task_with_context(
        task_id,
        decision,
        DeliveryTaskProjectionContext::minimal(
            None,
            DeliveryTargetObservation::unavailable(
                DeliveryTargetUnavailableReason::RuntimeUnavailable,
            ),
        ),
    )
}

pub(crate) fn project_delivery_task_with_context(
    task_id: TaskId,
    decision: DeliveryProjectionDecision,
    context: DeliveryTaskProjectionContext,
) -> DeliveryTaskProjection {
    match decision {
        DeliveryProjectionDecision::Eligible(actions) => DeliveryTaskProjection::new(
            task_id,
            DeliveryEligibility::Eligible,
            Vec::new(),
            context,
            stable_unique_actions(actions),
        ),
        DeliveryProjectionDecision::Ineligible(reasons) => DeliveryTaskProjection::new(
            task_id,
            DeliveryEligibility::Ineligible,
            stable_unique_reasons(reasons),
            context,
            Vec::new(),
        ),
        DeliveryProjectionDecision::Unavailable(reasons) => DeliveryTaskProjection::new(
            task_id,
            DeliveryEligibility::Unavailable,
            stable_unique_reasons(reasons),
            context,
            Vec::new(),
        ),
    }
}

fn stable_unique_actions(actions: Vec<DeliveryAllowedAction>) -> Vec<DeliveryAllowedAction> {
    actions.into_iter().fold(Vec::new(), |mut unique, action| {
        if !unique.contains(&action) {
            unique.push(action);
        }
        unique
    })
}

fn stable_unique_reasons(
    reasons: Vec<DeliveryEligibilityReason>,
) -> Vec<DeliveryEligibilityReason> {
    reasons.into_iter().fold(Vec::new(), |mut unique, reason| {
        if !unique.contains(&reason) {
            unique.push(reason);
        }
        unique
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_and_unavailable_projections_never_publish_actions() {
        let task_id = TaskId::new();
        let ineligible = project_delivery_task(
            task_id,
            DeliveryProjectionDecision::Ineligible(vec![
                DeliveryEligibilityReason::TaskActive,
                DeliveryEligibilityReason::TaskActive,
            ]),
        );
        assert_eq!(ineligible.eligibility(), DeliveryEligibility::Ineligible);
        assert_eq!(
            ineligible.reasons(),
            &[DeliveryEligibilityReason::TaskActive]
        );
        assert!(ineligible.allowed_actions().is_empty());

        let unavailable = project_delivery_task(
            task_id,
            DeliveryProjectionDecision::Unavailable(vec![
                DeliveryEligibilityReason::RuntimeObservationUnavailable,
            ]),
        );
        assert_eq!(unavailable.eligibility(), DeliveryEligibility::Unavailable);
        assert!(unavailable.allowed_actions().is_empty());
    }

    #[test]
    fn only_an_eligible_projection_offers_preflight() {
        let projection = project_delivery_task(
            TaskId::new(),
            DeliveryProjectionDecision::Eligible(vec![DeliveryAllowedAction::RunPreflight]),
        );
        assert_eq!(projection.eligibility(), DeliveryEligibility::Eligible);
        assert!(projection.reasons().is_empty());
        assert_eq!(
            projection.allowed_actions(),
            &[DeliveryAllowedAction::RunPreflight]
        );
    }

    #[test]
    fn eligible_projection_publishes_only_stable_unique_actions() {
        let task_id = TaskId::new();
        let projection = project_delivery_task(
            task_id,
            DeliveryProjectionDecision::Eligible(vec![
                DeliveryAllowedAction::RunPreflight,
                DeliveryAllowedAction::RunPreflight,
                DeliveryAllowedAction::AcceptMerge,
            ]),
        );
        assert_eq!(projection.eligibility(), DeliveryEligibility::Eligible);
        assert_eq!(
            projection.allowed_actions(),
            &[
                DeliveryAllowedAction::RunPreflight,
                DeliveryAllowedAction::AcceptMerge,
            ]
        );
    }
}
