package dev.agenticintegrationpatterns.orderdesk.model;

import java.time.Instant;

public record InventoryObservation(
        String warehouseId,
        int availableQuantity,
        Instant observedAt,
        String evidenceReference) {
}
