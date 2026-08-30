package dev.agenticintegrationpatterns.orderdesk.recovery;

import dev.agenticintegrationpatterns.orderdesk.effect.ReleaseInventoryReservationEffect;

public record SelectReservationReleaseRecovery(
        String tenantId,
        String planId,
        String failedEffectId,
        ReleaseInventoryReservationEffect compensation) {
    public SelectReservationReleaseRecovery {
        ResolutionPlanDefinition.require(tenantId, "tenantId", 120);
        ResolutionPlanDefinition.require(planId, "planId", 160);
        ResolutionPlanDefinition.require(failedEffectId, "failedEffectId", 160);
        if (compensation == null
                || !tenantId.equals(compensation.tenantId())
                || !planId.equals(compensation.planId())
                || !failedEffectId.equals(compensation.causedByEffectId())) {
            throw new IllegalArgumentException("compensation is not bound to this recovery decision");
        }
    }
}
