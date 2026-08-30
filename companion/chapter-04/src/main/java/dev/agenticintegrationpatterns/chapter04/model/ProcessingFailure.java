package dev.agenticintegrationpatterns.chapter04.model;

public record ProcessingFailure(
        FailureKind kind,
        String eventId,
        String correlationId,
        String tenantId,
        String caseId,
        String detail) {
}
