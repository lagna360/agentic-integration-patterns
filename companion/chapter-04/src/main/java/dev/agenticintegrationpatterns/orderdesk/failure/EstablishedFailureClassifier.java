package dev.agenticintegrationpatterns.orderdesk.failure;

import org.springframework.stereotype.Component;

import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.BusinessOutcome.INSUFFICIENT_EVIDENCE;
import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.OperatorAction.*;
import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.RetryEligibility.*;
import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.RunConsequence.*;
import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.StopCause.DEADLINE;

@Component
public final class EstablishedFailureClassifier implements FailureClassifier {
    public static final String TAXONOMY_VERSION = "orderdesk-failure-v1";

    @Override
    // tag::failure-classifier[]
    public FailureDecision classify(ClassificationObservation observation) {
        return switch (observation.kind()) {
            case INVALID_WORK -> decision(observation, REJECTED, KEEP_RUNNING,
                    NEVER_SAME_INPUT, FIX_OR_REISSUE_WORK, null, null);
            case RESULT_CONTRACT_INVALID -> decision(observation, REJECTED, KEEP_RUNNING,
                    GOVERNED_REPAIR_MAY_SUCCEED, REVIEW_PROVIDER_OR_ADAPTER, null, null);
            case POLICY_DENIED -> decision(observation, DENIED, KEEP_RUNNING,
                    NEW_AUTHORIZATION_OR_POLICY_REQUIRED, REVIEW_IDENTITY_OR_POLICY, null, null);
            case DEPENDENCY_UNAVAILABLE -> decision(observation, RETRY_ELIGIBLE, KEEP_RUNNING,
                    GOVERNED_RETRY_MAY_SUCCEED, OBSERVE_DEPENDENCY, null, null);
            case DEADLINE_EXCEEDED -> decision(observation, STOPPED, STOP,
                    NEW_WORK_REQUIRED, REVIEW_STOPPED_RUN, null, DEADLINE);
            case INSUFFICIENT_EVIDENCE -> decision(observation, BUSINESS_OUTCOME, COMPLETE,
                    NOT_APPLICABLE, REVIEW_EVIDENCE, INSUFFICIENT_EVIDENCE, null);
            case EXTERNAL_OUTCOME_UNKNOWN -> decision(observation,
                    RECONCILIATION_REQUIRED, KEEP_RUNNING,
                    RECONCILE_BEFORE_RETRY, RECONCILE_EXTERNAL_STATE, null, null);
        };
    }
    // end::failure-classifier[]

    private static FailureDecision decision(
            ClassificationObservation observation,
            FailureDecision.Disposition disposition,
            FailureDecision.RunConsequence runConsequence,
            FailureDecision.RetryEligibility retryEligibility,
            FailureDecision.OperatorAction operatorAction,
            FailureDecision.BusinessOutcome businessOutcome,
            FailureDecision.StopCause stopCause) {
        return new FailureDecision(
                observation.tenantId(), observation.runId(), observation.observationId(),
                TAXONOMY_VERSION, observation.stage(), observation.observedAt(),
                observation.kind(), disposition, runConsequence, retryEligibility,
                operatorAction, observation.reasonCode(), observation.evidenceRef(),
                businessOutcome, stopCause);
    }
}
