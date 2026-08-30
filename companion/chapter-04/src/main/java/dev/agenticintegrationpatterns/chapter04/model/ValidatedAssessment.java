package dev.agenticintegrationpatterns.chapter04.model;

public record ValidatedAssessment(
        AssessmentRequest request,
        FailureAssessment assessment,
        AssessmentProvenance provenance) {
}
