package dev.agenticintegrationpatterns.chapter04.model;

public record ManualReviewRequired(
        String eventId,
        String causedBy,
        String correlationId,
        String tenantId,
        String caseId,
        String reason,
        String provider,
        String model,
        String instructionVersion) implements AssessmentOutcome {
}
