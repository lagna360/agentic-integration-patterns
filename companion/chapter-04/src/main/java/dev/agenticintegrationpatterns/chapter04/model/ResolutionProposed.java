package dev.agenticintegrationpatterns.chapter04.model;

import java.util.List;

public record ResolutionProposed(
        String eventId,
        String causedBy,
        String correlationId,
        String tenantId,
        String caseId,
        ProposedResolution category,
        String rationale,
        List<String> evidenceReferences,
        String provider,
        String model,
        String instructionVersion) implements AssessmentOutcome {
}
