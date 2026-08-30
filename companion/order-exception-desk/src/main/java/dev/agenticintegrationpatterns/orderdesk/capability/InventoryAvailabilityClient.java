package dev.agenticintegrationpatterns.orderdesk.capability;

public interface InventoryAvailabilityClient {
    InventoryAvailabilityObservation read(
            String trustedTenantId,
            InventoryAvailabilityArguments arguments);
}
