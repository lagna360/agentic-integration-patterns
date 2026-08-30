package dev.agenticintegrationpatterns.chapter04.model;

import java.time.Instant;

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
