package dev.agenticintegrationpatterns.orderdesk.effect;

import java.time.Instant;

public record EffectReceipt(
        Disposition disposition,
        String tenantId,
        String effectId,
        State state,
        long version,
        int attemptCount,
        String targetIdempotencyKey,
        Instant idempotencyExpiresAt,
        String targetReference) {

    public enum Disposition {
        CREATED,
        DUPLICATE_SAME,
        IDENTITY_COLLISION,
        OUTCOME_RECORDED,
        RECONCILIATION_RECORDED,
        NO_EXECUTION
    }

    public enum State {
        RECORDED,
        DISPATCHING,
        ACCEPTED,
        SUCCEEDED,
        FAILED_CONFIRMED,
        UNKNOWN
    }
}
