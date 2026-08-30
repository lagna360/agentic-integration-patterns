package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.AssessmentRequest;
import dev.agenticintegrationpatterns.chapter04.model.GatewayAssessment;

public interface FailureAssessmentGateway {
    GatewayAssessment assess(AssessmentRequest request);
}
