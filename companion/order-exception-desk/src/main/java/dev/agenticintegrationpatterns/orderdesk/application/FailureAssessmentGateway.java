package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import dev.agenticintegrationpatterns.orderdesk.model.GatewayAssessment;

public interface FailureAssessmentGateway {
    GatewayAssessment assess(AssessmentRequest request);
}
