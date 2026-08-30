package dev.agenticintegrationpatterns.orderdesk.retry;

import dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt;
import dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static dev.agenticintegrationpatterns.orderdesk.retry.RetryPolicyDecision.Disposition.*;

/** An application-owned policy assembled from established retry controls. */
public final class EstablishedRetryPolicy {
    private final Clock clock;
    private final EqualJitterBackoff backoff;

    public EstablishedRetryPolicy(Clock clock, EqualJitterBackoff backoff) {
        this.clock = clock;
        this.backoff = backoff;
    }

    // tag::retry-policy-decision[]
    public RetryPolicyDecision decide(RetryPolicyRequest request) {
        if (request.failure().disposition() != FailureDecision.Disposition.RETRY_ELIGIBLE
                || request.failure().retryEligibility()
                != FailureDecision.RetryEligibility.GOVERNED_RETRY_MAY_SUCCEED) {
            return RetryPolicyDecision.refused(FAILURE_NOT_RETRYABLE,
                    "FAILURE_CLASSIFICATION_FORBIDS_RETRY");
        }
        var effectRefusal = refuseUnsafeEffect(request.effect());
        if (effectRefusal != null) {
            return effectRefusal;
        }
        if (request.attemptsUsed() >= request.maxAttempts()) {
            return RetryPolicyDecision.refused(ATTEMPT_BUDGET_EXHAUSTED,
                    "ATTEMPT_BUDGET_EXHAUSTED");
        }
        if (wouldExceed(request.tokensUsed(), request.nextAttemptTokens(), request.maxTokens())) {
            return RetryPolicyDecision.refused(TOKEN_BUDGET_EXHAUSTED,
                    "TOKEN_BUDGET_EXHAUSTED");
        }
        if (wouldExceed(request.costUsedMicros(), request.nextAttemptCostMicros(),
                request.maxCostMicros())) {
            return RetryPolicyDecision.refused(COST_BUDGET_EXHAUSTED,
                    "COST_BUDGET_EXHAUSTED");
        }

        Instant now = clock.instant();
        var delay = backoff.delay(
                request.baseBackoff(), request.maximumBackoff(), request.attemptsUsed());
        Instant policyNotBefore = now.plus(delay);
        Instant notBefore = later(policyNotBefore, request.trustedRetryNotBefore());
        Duration remainingForAttempt = Duration.between(
                notBefore, request.absoluteDeadline()).minus(request.settlementReserve());
        Duration attemptTimeout = shorter(
                request.configuredAttemptTimeout(), remainingForAttempt);
        if (attemptTimeout.compareTo(request.minimumUsefulAttemptTimeout()) < 0) {
            return RetryPolicyDecision.refused(DEADLINE_EXHAUSTED,
                    "DEADLINE_CANNOT_FIT_ANOTHER_ATTEMPT");
        }
        Instant mustRemainValidUntil = notBefore.plus(attemptTimeout);
        if (request.effect() != null
                && (request.effect().idempotencyExpiresAt() == null
                || !mustRemainValidUntil.isBefore(request.effect().idempotencyExpiresAt()))) {
            return RetryPolicyDecision.refused(IDEMPOTENCY_KEY_EXPIRED,
                    "TARGET_IDEMPOTENCY_WINDOW_TOO_SHORT");
        }
        return new RetryPolicyDecision(SCHEDULED, "GOVERNED_RETRY_SCHEDULED",
                notBefore, delay, attemptTimeout);
    }
    // end::retry-policy-decision[]

    private static RetryPolicyDecision refuseUnsafeEffect(EffectReceipt effect) {
        if (effect == null) {
            return null;
        }
        return switch (effect.state()) {
            case UNKNOWN -> RetryPolicyDecision.refused(EFFECT_OUTCOME_UNRESOLVED,
                    "RECONCILE_UNKNOWN_EFFECT_BEFORE_RETRY");
            case ACCEPTED -> RetryPolicyDecision.refused(EFFECT_ALREADY_ACCEPTED,
                    "OBSERVE_ACCEPTED_EFFECT_BEFORE_RETRY");
            case FAILED_CONFIRMED -> {
                if (effect.targetIdempotencyKey() == null
                        || effect.targetIdempotencyKey().isBlank()) {
                    yield RetryPolicyDecision.refused(EFFECT_RETRY_PROTECTION_MISSING,
                            "ORIGINAL_TARGET_IDEMPOTENCY_KEY_MISSING");
                }
                if (effect.idempotencyExpiresAt() == null) {
                    yield RetryPolicyDecision.refused(EFFECT_RETRY_PROTECTION_MISSING,
                            "TARGET_IDEMPOTENCY_EXPIRY_MISSING");
                }
                yield null;
            }
            default -> RetryPolicyDecision.refused(EFFECT_NOT_CONFIRMED_FAILED,
                    "EFFECT_IS_NOT_CONFIRMED_NOT_APPLIED");
        };
    }

    private static boolean wouldExceed(long used, long next, long maximum) {
        return next > maximum - used;
    }

    private static Instant later(Instant left, Instant right) {
        return right != null && right.isAfter(left) ? right : left;
    }

    private static Duration shorter(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }
}
