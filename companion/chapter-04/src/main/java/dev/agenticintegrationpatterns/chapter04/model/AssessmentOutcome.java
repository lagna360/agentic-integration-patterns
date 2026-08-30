package dev.agenticintegrationpatterns.chapter04.model;

public sealed interface AssessmentOutcome permits ResolutionProposed, ManualReviewRequired {
    String eventId();
    String correlationId();
    String tenantId();
    String caseId();
}
