package dev.agenticintegrationpatterns.orderdesk.capability;

import java.time.Instant;

public record InventoryAvailabilityObservation(
        String tenantId,
        String sku,
        String locationId,
        int availableUnits,
        String sourceVersion,
        Instant observedAt,
        Instant validUntil) {
}
