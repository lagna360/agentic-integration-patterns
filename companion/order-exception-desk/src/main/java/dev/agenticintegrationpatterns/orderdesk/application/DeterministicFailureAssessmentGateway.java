package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.AssessmentDisposition;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentProvenance;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import dev.agenticintegrationpatterns.orderdesk.model.FailureAssessment;
import dev.agenticintegrationpatterns.orderdesk.model.GatewayAssessment;
import dev.agenticintegrationpatterns.orderdesk.model.ProposedResolution;
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
                new AssessmentProvenance("fixture", "deterministic-baseline", "order-exception-assessment-v1"));
    }
}
