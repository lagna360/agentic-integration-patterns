package dev.agenticintegrationpatterns.orderdesk.model;

public record ProcessingFailure(
        FailureKind kind,
        String eventId,
        String correlationId,
        String tenantId,
        String caseId,
        String detail) {
}
