package dev.agenticintegrationpatterns.chapter04.model;

public record OrderExceptionCase(
        String caseId,
        String tenantId,
        String orderId,
        String sku,
        long version,
        String status) {
}
