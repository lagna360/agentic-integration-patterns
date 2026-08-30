package dev.agenticintegrationpatterns.orderdesk.model;

import java.util.List;

public record FailureAssessment(
        AssessmentDisposition disposition,
        ProposedResolution proposedResolution,
        String rationale,
        List<String> evidenceReferences) {
    public FailureAssessment {
        evidenceReferences = evidenceReferences == null ? List.of() : List.copyOf(evidenceReferences);
    }
}
