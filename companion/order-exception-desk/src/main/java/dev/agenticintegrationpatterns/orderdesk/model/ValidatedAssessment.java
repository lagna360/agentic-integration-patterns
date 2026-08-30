package dev.agenticintegrationpatterns.orderdesk.model;

public record ValidatedAssessment(
        AssessmentRequest request,
        FailureAssessment assessment,
        AssessmentProvenance provenance) {
}
