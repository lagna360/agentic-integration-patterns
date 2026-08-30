package dev.agenticintegrationpatterns.orderdesk.retry;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public record RetryPolicyDecision(
        Disposition disposition,
        String reasonCode,
        Instant notBefore,
        Duration selectedBackoff,
        Duration attemptTimeout) {

    public RetryPolicyDecision {
        Objects.requireNonNull(disposition, "disposition");
        Objects.requireNonNull(reasonCode, "reasonCode");
        if (disposition == Disposition.SCHEDULED) {
            Objects.requireNonNull(notBefore, "notBefore");
            Objects.requireNonNull(selectedBackoff, "selectedBackoff");
            Objects.requireNonNull(attemptTimeout, "attemptTimeout");
        } else if (notBefore != null || selectedBackoff != null || attemptTimeout != null) {
            throw new IllegalArgumentException("refusal cannot carry a retry time");
        }
    }

    public static RetryPolicyDecision refused(Disposition disposition, String reasonCode) {
        return new RetryPolicyDecision(disposition, reasonCode, null, null, null);
    }

    public enum Disposition {
        SCHEDULED,
        FAILURE_NOT_RETRYABLE,
        EFFECT_OUTCOME_UNRESOLVED,
        EFFECT_ALREADY_ACCEPTED,
        EFFECT_NOT_CONFIRMED_FAILED,
        EFFECT_RETRY_PROTECTION_MISSING,
        IDEMPOTENCY_KEY_EXPIRED,
        DEADLINE_EXHAUSTED,
        ATTEMPT_BUDGET_EXHAUSTED,
        TOKEN_BUDGET_EXHAUSTED,
        COST_BUDGET_EXHAUSTED
    }
}
