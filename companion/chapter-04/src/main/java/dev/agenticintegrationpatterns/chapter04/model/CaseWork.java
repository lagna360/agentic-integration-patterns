package dev.agenticintegrationpatterns.chapter04.model;

public record CaseWork(
        InventoryShortfallDetected event,
        OrderExceptionCase orderExceptionCase) {
}
