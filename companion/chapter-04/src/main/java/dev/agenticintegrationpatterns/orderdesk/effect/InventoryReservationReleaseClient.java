package dev.agenticintegrationpatterns.orderdesk.effect;

public interface InventoryReservationReleaseClient {
    InventoryReservationClient.InvocationResult release(ReleaseRequest request);

    record ReleaseRequest(
            String tenantId,
            String effectId,
            String attemptId,
            String idempotencyKey,
            String reservationReference,
            String warehouseId,
            String sku,
            int quantity) { }
}
