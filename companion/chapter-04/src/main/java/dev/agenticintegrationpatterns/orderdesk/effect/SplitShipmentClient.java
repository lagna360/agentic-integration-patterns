package dev.agenticintegrationpatterns.orderdesk.effect;

public interface SplitShipmentClient {
    InventoryReservationClient.InvocationResult create(CreateRequest request);

    record CreateRequest(
            String tenantId,
            String effectId,
            String attemptId,
            String idempotencyKey,
            String orderId,
            long expectedOrderVersion,
            String reservationReference,
            String warehouseId,
            String sku,
            int quantity) { }
}
