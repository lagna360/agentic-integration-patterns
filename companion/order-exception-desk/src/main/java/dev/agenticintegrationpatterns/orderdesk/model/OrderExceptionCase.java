package dev.agenticintegrationpatterns.orderdesk.model;

public record OrderExceptionCase(
        String caseId,
        String tenantId,
        String orderId,
        String sku,
        long version,
        String status) {
}
