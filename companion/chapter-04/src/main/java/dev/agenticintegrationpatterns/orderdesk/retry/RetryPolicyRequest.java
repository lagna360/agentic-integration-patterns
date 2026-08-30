package dev.agenticintegrationpatterns.orderdesk.retry;

import dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt;
import dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.regex.Pattern;

/** Trusted application state for one retry decision; this is not a wire contract. */
public record RetryPolicyRequest(
        String tenantId,
        String scheduleId,
        String operationKey,
        FailureDecision failure,
        EffectReceipt effect,
        Instant absoluteDeadline,
        Instant trustedRetryNotBefore,
        Duration baseBackoff,
        Duration maximumBackoff,
        Duration configuredAttemptTimeout,
        Duration settlementReserve,
        Duration minimumUsefulAttemptTimeout,
        int maxAttempts,
        int attemptsUsed,
        long maxTokens,
        long tokensUsed,
        long nextAttemptTokens,
        long maxCostMicros,
        long costUsedMicros,
        long nextAttemptCostMicros) {

    private static final Pattern ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,239}");

    public RetryPolicyRequest {
        tenantId = requireId(tenantId, "tenantId", 120);
        scheduleId = requireId(scheduleId, "scheduleId", 160);
        operationKey = requireId(operationKey, "operationKey", 240);
        Objects.requireNonNull(failure, "failure");
        Objects.requireNonNull(absoluteDeadline, "absoluteDeadline");
        Objects.requireNonNull(baseBackoff, "baseBackoff");
        Objects.requireNonNull(maximumBackoff, "maximumBackoff");
        Objects.requireNonNull(configuredAttemptTimeout, "configuredAttemptTimeout");
        Objects.requireNonNull(settlementReserve, "settlementReserve");
        Objects.requireNonNull(minimumUsefulAttemptTimeout, "minimumUsefulAttemptTimeout");
        if (!tenantId.equals(failure.tenantId())) {
            throw new IllegalArgumentException("failure tenant must match retry tenant");
        }
        if (effect != null && !tenantId.equals(effect.tenantId())) {
            throw new IllegalArgumentException("effect tenant must match retry tenant");
        }
        if (baseBackoff.isNegative() || baseBackoff.isZero()
                || maximumBackoff.compareTo(baseBackoff) < 0
                || configuredAttemptTimeout.isNegative() || configuredAttemptTimeout.isZero()
                || settlementReserve.isNegative()
                || minimumUsefulAttemptTimeout.isNegative()
                || minimumUsefulAttemptTimeout.isZero()
                || minimumUsefulAttemptTimeout.compareTo(configuredAttemptTimeout) > 0) {
            throw new IllegalArgumentException("backoff and execution windows are invalid");
        }
        if (maxAttempts < 1 || attemptsUsed < 0 || attemptsUsed > maxAttempts) {
            throw new IllegalArgumentException("attempt budget is invalid");
        }
        requireBudget(maxTokens, tokensUsed, nextAttemptTokens, "token");
        requireBudget(maxCostMicros, costUsedMicros, nextAttemptCostMicros, "cost");
    }

    private static String requireId(String value, String field, int limit) {
        if (value == null || value.length() > limit || !ID.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " is not a bounded identifier");
        }
        return value;
    }

    private static void requireBudget(long maximum, long used, long next, String name) {
        if (maximum < 0 || used < 0 || next < 0 || used > maximum) {
            throw new IllegalArgumentException(name + " budget is invalid");
        }
    }
}
