package dev.agenticintegrationpatterns.orderdesk.model;

public record CaseWork(
        InventoryShortfallDetected event,
        OrderExceptionCase orderExceptionCase) {
}
