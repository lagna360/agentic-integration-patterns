package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.AssessmentDisposition;
import dev.agenticintegrationpatterns.chapter04.model.AssessmentProvenance;
import dev.agenticintegrationpatterns.chapter04.model.AssessmentRequest;
import dev.agenticintegrationpatterns.chapter04.model.FailureAssessment;
import dev.agenticintegrationpatterns.chapter04.model.GatewayAssessment;
import dev.agenticintegrationpatterns.chapter04.model.ProposedResolution;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Profile("!openai")
public class DeterministicFailureAssessmentGateway implements FailureAssessmentGateway {
    @Override
    public GatewayAssessment assess(AssessmentRequest request) {
        var alternate = request.context().inventory().stream()
                .filter(item -> !item.warehouseId().equals(request.caseWork().event().warehouseId()))
                .filter(item -> item.availableQuantity() > 0)
                .findFirst();

        var assessment = alternate
                .<FailureAssessment>map(item -> new FailureAssessment(
                        AssessmentDisposition.PROPOSE_RESOLUTION,
                        ProposedResolution.SPLIT_SHIPMENT,
                        "Verified stock exists at an alternate warehouse.",
                        List.of(item.evidenceReference())))
                .orElseGet(() -> new FailureAssessment(
                        AssessmentDisposition.REQUEST_MANUAL_REVIEW,
                        ProposedResolution.NONE,
                        "No verified alternate stock was found.",
                        List.of()));

        return new GatewayAssessment(
                assessment,
                new AssessmentProvenance("fixture", "deterministic-baseline", "chapter-04-v1"));
    }
}
