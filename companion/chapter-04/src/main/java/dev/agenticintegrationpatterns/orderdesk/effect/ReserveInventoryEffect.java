package dev.agenticintegrationpatterns.orderdesk.effect;

/**
 * An already-governed application intent. It is not a model tool request or a
 * public wire contract.
 */
public record ReserveInventoryEffect(
        String tenantId,
        String runId,
        String caseId,
        String effectId,
        String decisionRef,
        String policySnapshotRef,
        String warehouseId,
        String sku,
        int quantity) {

    public ReserveInventoryEffect {
        requireText(tenantId, "tenantId", 120);
        requireText(runId, "runId", 160);
        requireText(caseId, "caseId", 160);
        requireText(effectId, "effectId", 160);
        requireText(decisionRef, "decisionRef", 600);
        requireText(policySnapshotRef, "policySnapshotRef", 600);
        requireText(warehouseId, "warehouseId", 160);
        requireText(sku, "sku", 160);
        requireText(resourceKey(warehouseId, sku), "targetResourceKey", 320);
        if (quantity < 1 || quantity > 10_000) {
            throw new IllegalArgumentException("quantity must be between 1 and 10000");
        }
    }

    public String targetResourceKey() {
        return resourceKey(warehouseId, sku);
    }

    static void requireText(String value, String name, int maxLength) {
        if (value == null || value.isBlank() || value.length() > maxLength) {
            throw new IllegalArgumentException(name + " is missing or too long");
        }
    }

    static void requireOptionalText(String value, String name, int maxLength) {
        if (value != null) {
            requireText(value, name, maxLength);
        }
    }

    private static String resourceKey(String warehouseId, String sku) {
        return "warehouse/" + warehouseId + "/sku/" + sku;
    }
}
