package dev.agenticintegrationpatterns.orderdesk.effect;

import dev.agenticintegrationpatterns.orderdesk.recovery.RecoveryAuthority;

/** A new governed effect that semantically compensates one successful reservation. */
public record ReleaseInventoryReservationEffect(
        String tenantId,
        String runId,
        String caseId,
        String planId,
        String effectId,
        String causedByEffectId,
        String compensatesEffectId,
        String decisionRef,
        String policySnapshotRef,
        String targetContractRef,
        String reservationReference,
        String warehouseId,
        String sku,
        int quantity,
        RecoveryAuthority authority) {

    public ReleaseInventoryReservationEffect {
        ReserveInventoryEffect.requireText(tenantId, "tenantId", 120);
        ReserveInventoryEffect.requireText(runId, "runId", 160);
        ReserveInventoryEffect.requireText(caseId, "caseId", 160);
        ReserveInventoryEffect.requireText(planId, "planId", 160);
        ReserveInventoryEffect.requireText(effectId, "effectId", 160);
        ReserveInventoryEffect.requireText(causedByEffectId, "causedByEffectId", 160);
        ReserveInventoryEffect.requireText(compensatesEffectId, "compensatesEffectId", 160);
        ReserveInventoryEffect.requireText(decisionRef, "decisionRef", 600);
        ReserveInventoryEffect.requireText(policySnapshotRef, "policySnapshotRef", 600);
        ReserveInventoryEffect.requireText(targetContractRef, "targetContractRef", 240);
        ReserveInventoryEffect.requireText(reservationReference, "reservationReference", 600);
        ReserveInventoryEffect.requireText(warehouseId, "warehouseId", 160);
        ReserveInventoryEffect.requireText(sku, "sku", 160);
        if (quantity < 1 || quantity > 10_000) {
            throw new IllegalArgumentException("quantity must be between 1 and 10000");
        }
        if (authority == null
                || !authority.tenantId().equals(tenantId)
                || !authority.planId().equals(planId)
                || !authority.effectId().equals(effectId)
                || !authority.compensatesEffectId().equals(compensatesEffectId)
                || !authority.effectType().equals("RELEASE_INVENTORY_RESERVATION")
                || !authority.targetResourceKey().equals(
                        "warehouse/" + warehouseId + "/reservations/" + reservationReference)) {
            throw new IllegalArgumentException("recovery authority does not bind the exact effect");
        }
    }

    public String targetResourceKey() {
        return "warehouse/" + warehouseId + "/reservations/" + reservationReference;
    }
}
