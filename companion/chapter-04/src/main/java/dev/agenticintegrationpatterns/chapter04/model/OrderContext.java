package dev.agenticintegrationpatterns.chapter04.model;

import java.util.List;

public record OrderContext(
        String orderId,
        List<InventoryObservation> inventory,
        List<EvidenceReference> evidence) {
    public OrderContext {
        inventory = List.copyOf(inventory);
        evidence = List.copyOf(evidence);
    }
}
