package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.AssessmentDisposition;
import dev.agenticintegrationpatterns.orderdesk.model.ValidatedAssessment;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.springframework.stereotype.Component;

@Component
public class AssessmentPredicates {
    public Predicate proposesResolution() {
        return exchange -> exchange.getMessage().getBody(ValidatedAssessment.class)
                .assessment().disposition() == AssessmentDisposition.PROPOSE_RESOLUTION;
    }
}
