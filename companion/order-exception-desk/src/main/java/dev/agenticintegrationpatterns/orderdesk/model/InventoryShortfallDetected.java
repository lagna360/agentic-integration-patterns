package dev.agenticintegrationpatterns.orderdesk.model;

import java.time.Instant;

// tag::ch4-event-contract[]
public record InventoryShortfallDetected(
        int schemaVersion,
        String eventId,
        String correlationId,
        String tenantId,
        String type,
        Instant occurredAt,
        String orderId,
        String sku,
        int requestedQuantity,
        int availableQuantity,
        String warehouseId) {
}
// end::ch4-event-contract[]
