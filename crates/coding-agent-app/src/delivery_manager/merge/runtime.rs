use super::super::runtime::DeliveryRuntimeAuthentication;
use super::validation::ValidatedAccept;
use super::*;

pub(super) struct AuthenticatedAccept {
    pub(super) validated: ValidatedAccept,
    pub(super) session: Arc<dyn DeliveryLiveRuntimeSession>,
    pub(super) authentication: DeliveryRuntimeAuthentication,
}

pub(super) async fn authenticate(
    flow: &AcceptFlow,
    validated: ValidatedAccept,
) -> Result<AuthenticatedAccept, WorkerFinish> {
    let Some(registry) = flow.dependencies.live_runtime_registry.as_ref() else {
        return Err(validated.routed.admission.clean(
            &flow.response,
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::RuntimeUnavailable,
            ),
        ));
    };
    let operation = &validated.routed.context.operation;
    let session = match timeout(
        LIVE_RUNTIME_STAGE_TIMEOUT,
        registry.open_live_session(&validated.routed.context.snapshot),
    )
    .await
    {
        Ok(Ok(session)) => session,
        Ok(Err(DeliveryLiveRuntimeError::ProcessCleanupUnproven)) => {
            return Err(validated.routed.admission.retain(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
                ),
            ));
        }
        Ok(Err(DeliveryLiveRuntimeError::ReconciliationRequired(reason))) => {
            let stage = reconcile_operation(flow.dependencies.as_ref(), operation, reason).await;
            let outcome = merge_reconciliation_admission_outcome(reason);
            send_accept_response(&flow.response, outcome.clone());
            return Err(validated
                .routed
                .admission
                .finish(stage)
                .with_accept_fallback(outcome));
        }
        Ok(Err(DeliveryLiveRuntimeError::Unavailable)) | Err(_) => {
            return Err(validated.routed.admission.clean(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RuntimeUnavailable,
                ),
            ));
        }
    };
    let authentication = match timeout(
        LIVE_RUNTIME_STAGE_TIMEOUT,
        session.authenticate_accept(&flow.command),
    )
    .await
    {
        Ok(Ok(authentication)) => authentication,
        Ok(Err(DeliveryAcceptAuthenticationError::Rejected(reason))) => {
            return Err(validated
                .routed
                .admission
                .clean(&flow.response, rejected_accept_outcome(reason)));
        }
        Ok(Err(DeliveryAcceptAuthenticationError::Stale(reason))) => {
            return Err(
                super::persist::persist_stale_authentication(flow, validated, reason).await,
            );
        }
        Ok(Err(DeliveryAcceptAuthenticationError::MergeConflict)) => {
            return Err(validated.routed.admission.clean(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::MergeConflict),
            ));
        }
        Ok(Err(DeliveryAcceptAuthenticationError::CommandTimedOut)) => {
            return Err(validated.routed.admission.clean(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::CommandTimedOut,
                ),
            ));
        }
        Ok(Err(DeliveryAcceptAuthenticationError::ProcessCleanupUnproven)) => {
            return Err(validated.routed.admission.retain(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
                ),
            ));
        }
        Ok(Err(DeliveryAcceptAuthenticationError::ReconciliationRequired(reason))) => {
            let stage = reconcile_operation(flow.dependencies.as_ref(), operation, reason).await;
            let outcome = merge_reconciliation_admission_outcome(reason);
            send_accept_response(&flow.response, outcome.clone());
            return Err(validated
                .routed
                .admission
                .finish(stage)
                .with_accept_fallback(outcome));
        }
        Ok(Err(DeliveryAcceptAuthenticationError::Unavailable)) | Err(_) => {
            return Err(validated.routed.admission.clean(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RuntimeUnavailable,
                ),
            ));
        }
    };
    if !authentication.authorizes_accept(
        &validated.routed.context.snapshot,
        &flow.command,
        validated.routed.admission.lease.coordination_key(),
    ) || !authentication.authorizes_operation(operation)
    {
        return Err(validated
            .routed
            .admission
            .poison(&flow.response, inconsistent_accept_outcome()));
    }

    Ok(AuthenticatedAccept {
        validated,
        session,
        authentication,
    })
}
