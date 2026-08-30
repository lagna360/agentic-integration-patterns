package dev.agenticintegrationpatterns.chapter04.model;

import java.time.Instant;

public record InventoryObservation(
        String warehouseId,
        int availableQuantity,
        Instant observedAt,
        String evidenceReference) {
}
