package dev.agenticintegrationpatterns.orderdesk.effect;

public record ReconcileEffect(
        String tenantId,
        String effectId,
        String observationId) {

    public ReconcileEffect {
        ReserveInventoryEffect.requireText(tenantId, "tenantId", 120);
        ReserveInventoryEffect.requireText(effectId, "effectId", 160);
        ReserveInventoryEffect.requireText(observationId, "observationId", 200);
    }
}
