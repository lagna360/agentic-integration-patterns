package dev.agenticintegrationpatterns.orderdesk.effect;

import java.time.Duration;

public record ExecuteEffect(
        String tenantId,
        String effectId,
        String workerId,
        Duration leaseDuration) {

    public ExecuteEffect {
        ReserveInventoryEffect.requireText(tenantId, "tenantId", 120);
        ReserveInventoryEffect.requireText(effectId, "effectId", 160);
        ReserveInventoryEffect.requireText(workerId, "workerId", 200);
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
    }
}
