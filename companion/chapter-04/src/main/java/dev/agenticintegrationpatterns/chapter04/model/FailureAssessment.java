package dev.agenticintegrationpatterns.chapter04.model;

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
