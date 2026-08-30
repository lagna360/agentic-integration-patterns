package dev.agenticintegrationpatterns.orderdesk.effect;

import java.time.Instant;

/** A governed second forward effect in the canonical split-shipment plan. */
public record CreateSplitShipmentEffect(
        String tenantId,
        String runId,
        String caseId,
        String effectId,
        String decisionRef,
        String policySnapshotRef,
        String causedByEffectId,
        String authorityRef,
        Instant authorityValidUntil,
        String evidenceSha256,
        String configurationRef,
        String orderId,
        long expectedOrderVersion,
        String reservationReference,
        String warehouseId,
        String sku,
        int quantity) {

    public CreateSplitShipmentEffect {
        ReserveInventoryEffect.requireText(tenantId, "tenantId", 120);
        ReserveInventoryEffect.requireText(runId, "runId", 160);
        ReserveInventoryEffect.requireText(caseId, "caseId", 160);
        ReserveInventoryEffect.requireText(effectId, "effectId", 160);
        ReserveInventoryEffect.requireText(decisionRef, "decisionRef", 600);
        ReserveInventoryEffect.requireText(policySnapshotRef, "policySnapshotRef", 600);
        ReserveInventoryEffect.requireText(causedByEffectId, "causedByEffectId", 160);
        ReserveInventoryEffect.requireText(authorityRef, "authorityRef", 600);
        if (authorityValidUntil == null) {
            throw new IllegalArgumentException("authorityValidUntil is required");
        }
        ReserveInventoryEffect.requireText(evidenceSha256, "evidenceSha256", 64);
        ReserveInventoryEffect.requireText(configurationRef, "configurationRef", 240);
        ReserveInventoryEffect.requireText(orderId, "orderId", 160);
        if (expectedOrderVersion < 1) {
            throw new IllegalArgumentException("expectedOrderVersion must be positive");
        }
        ReserveInventoryEffect.requireText(reservationReference, "reservationReference", 600);
        ReserveInventoryEffect.requireText(warehouseId, "warehouseId", 160);
        ReserveInventoryEffect.requireText(sku, "sku", 160);
        if (!evidenceSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("evidenceSha256 must be lowercase SHA-256");
        }
        if (quantity < 1 || quantity > 10_000) {
            throw new IllegalArgumentException("quantity must be between 1 and 10000");
        }
    }

    public String targetResourceKey() {
        return "order/" + orderId + "/split-shipment";
    }
}
