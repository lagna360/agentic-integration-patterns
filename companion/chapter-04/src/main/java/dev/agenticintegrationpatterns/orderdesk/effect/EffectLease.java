package dev.agenticintegrationpatterns.orderdesk.effect;

import java.time.Instant;

public record EffectLease(
        String tenantId,
        String runId,
        String caseId,
        String effectId,
        String attemptId,
        int attemptNumber,
        String owner,
        long version,
        long fenceToken,
        Instant leaseUntil,
        String targetIdempotencyKey,
        Instant idempotencyExpiresAt,
        String effectType,
        String warehouseId,
        String sku,
        int quantity,
        String orderId,
        long expectedOrderVersion,
        String reservationReference) {
}
