package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.AssessmentDisposition;
import dev.agenticintegrationpatterns.chapter04.model.ValidatedAssessment;
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
