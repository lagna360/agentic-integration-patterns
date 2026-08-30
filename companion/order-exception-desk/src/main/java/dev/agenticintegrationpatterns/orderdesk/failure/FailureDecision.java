package dev.agenticintegrationpatterns.orderdesk.failure;

import java.time.Instant;
import java.util.Objects;

/** A local classification result, not a retry schedule or a wire standard. */
public record FailureDecision(
        String tenantId,
        String runId,
        String observationId,
        String taxonomyVersion,
        ClassificationObservation.Stage stage,
        Instant observedAt,
        ClassificationObservation.Kind category,
        Disposition disposition,
        RunConsequence runConsequence,
        RetryEligibility retryEligibility,
        OperatorAction operatorAction,
        String reasonCode,
        String evidenceRef,
        BusinessOutcome businessOutcome,
        StopCause stopCause) {

    public FailureDecision {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(observationId, "observationId");
        Objects.requireNonNull(taxonomyVersion, "taxonomyVersion");
        Objects.requireNonNull(stage, "stage");
        Objects.requireNonNull(observedAt, "observedAt");
        Objects.requireNonNull(category, "category");
        Objects.requireNonNull(disposition, "disposition");
        Objects.requireNonNull(runConsequence, "runConsequence");
        Objects.requireNonNull(retryEligibility, "retryEligibility");
        Objects.requireNonNull(operatorAction, "operatorAction");
        Objects.requireNonNull(reasonCode, "reasonCode");
        Objects.requireNonNull(evidenceRef, "evidenceRef");

        if ((runConsequence == RunConsequence.COMPLETE) != (businessOutcome != null)) {
            throw new IllegalArgumentException(
                    "COMPLETE requires one business outcome, and other consequences forbid it");
        }
        if ((runConsequence == RunConsequence.STOP) != (stopCause != null)) {
            throw new IllegalArgumentException(
                    "STOP requires one stop cause, and other consequences forbid it");
        }
    }

    public enum Disposition {
        REJECTED,
        DENIED,
        RETRY_ELIGIBLE,
        STOPPED,
        BUSINESS_OUTCOME,
        RECONCILIATION_REQUIRED
    }

    public enum RunConsequence {
        KEEP_RUNNING,
        COMPLETE,
        STOP
    }

    public enum RetryEligibility {
        NEVER_SAME_INPUT,
        GOVERNED_REPAIR_MAY_SUCCEED,
        NEW_AUTHORIZATION_OR_POLICY_REQUIRED,
        GOVERNED_RETRY_MAY_SUCCEED,
        NEW_WORK_REQUIRED,
        NOT_APPLICABLE,
        RECONCILE_BEFORE_RETRY
    }

    public enum OperatorAction {
        FIX_OR_REISSUE_WORK,
        REVIEW_PROVIDER_OR_ADAPTER,
        REVIEW_IDENTITY_OR_POLICY,
        OBSERVE_DEPENDENCY,
        REVIEW_STOPPED_RUN,
        REVIEW_EVIDENCE,
        RECONCILE_EXTERNAL_STATE
    }

    public enum BusinessOutcome {
        INSUFFICIENT_EVIDENCE
    }

    public enum StopCause {
        DEADLINE
    }
}
