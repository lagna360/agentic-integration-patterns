package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import dev.agenticintegrationpatterns.orderdesk.model.GatewayAssessment;

// tag::ch4-gateway-port[]
public interface FailureAssessmentGateway {
    GatewayAssessment assess(AssessmentRequest request);
}
// end::ch4-gateway-port[]
