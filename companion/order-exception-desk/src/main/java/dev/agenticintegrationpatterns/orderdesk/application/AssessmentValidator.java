package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.AssessmentDisposition;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import dev.agenticintegrationpatterns.orderdesk.model.GatewayAssessment;
import dev.agenticintegrationpatterns.orderdesk.model.ProposedResolution;
import dev.agenticintegrationpatterns.orderdesk.model.ValidatedAssessment;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

@Component
public class AssessmentValidator {
    public ValidatedAssessment validate(AssessmentRequest request, GatewayAssessment gatewayResult) {
        if (gatewayResult == null || gatewayResult.assessment() == null || gatewayResult.provenance() == null) {
            throw new InvalidAssessmentException("Gateway returned an incomplete assessment");
        }
        var assessment = gatewayResult.assessment();
        var provenance = gatewayResult.provenance();
        if (assessment.disposition() == null || assessment.proposedResolution() == null
                || assessment.rationale() == null || assessment.rationale().isBlank()) {
            throw new InvalidAssessmentException("Assessment is missing required fields");
        }
        if (!text(provenance.provider()) || !text(provenance.model())
                || !text(provenance.instructionVersion())) {
            throw new InvalidAssessmentException("Assessment provenance is missing required fields");
        }
        if (assessment.disposition() == AssessmentDisposition.PROPOSE_RESOLUTION
                && assessment.proposedResolution() == ProposedResolution.NONE) {
            throw new InvalidAssessmentException("A proposal requires a resolution category");
        }
        if (assessment.disposition() == AssessmentDisposition.REQUEST_MANUAL_REVIEW
                && assessment.proposedResolution() != ProposedResolution.NONE) {
            throw new InvalidAssessmentException("Manual review cannot carry a proposed action");
        }

        Set<String> freshEvidence = request.context().evidence().stream()
                .filter(reference -> reference.fresh())
                .map(reference -> reference.reference())
                .collect(Collectors.toSet());
        if (!freshEvidence.containsAll(assessment.evidenceReferences())) {
            throw new InvalidAssessmentException("Assessment cited unknown or stale evidence");
        }
        if (assessment.disposition() == AssessmentDisposition.PROPOSE_RESOLUTION
                && assessment.evidenceReferences().isEmpty()) {
            throw new InvalidAssessmentException("A proposal must cite verified evidence");
        }
        return new ValidatedAssessment(request, assessment, gatewayResult.provenance());
    }

    private static boolean text(String value) {
        return value != null && !value.isBlank();
    }
}
