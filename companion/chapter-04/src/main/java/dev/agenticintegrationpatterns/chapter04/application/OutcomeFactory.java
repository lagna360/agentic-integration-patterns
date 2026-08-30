package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.ManualReviewRequired;
import dev.agenticintegrationpatterns.chapter04.model.ResolutionProposed;
import dev.agenticintegrationpatterns.chapter04.model.ValidatedAssessment;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
public class OutcomeFactory {
    public Processor proposal() {
        return exchange -> {
            var value = exchange.getMessage().getBody(ValidatedAssessment.class);
            var event = value.request().caseWork().event();
            var exceptionCase = value.request().caseWork().orderExceptionCase();
            exchange.getMessage().setBody(new ResolutionProposed(
                    outcomeEventId("ResolutionProposed", event.eventId(), exceptionCase.caseId(),
                            exceptionCase.version()),
                    event.eventId(), event.correlationId(), event.tenantId(), exceptionCase.caseId(),
                    value.assessment().proposedResolution(), value.assessment().rationale(),
                    value.assessment().evidenceReferences(), value.provenance().provider(),
                    value.provenance().model(),
                    value.provenance().instructionVersion()));
        };
    }

    public Processor manualReview() {
        return exchange -> {
            var value = exchange.getMessage().getBody(ValidatedAssessment.class);
            var event = value.request().caseWork().event();
            var exceptionCase = value.request().caseWork().orderExceptionCase();
            exchange.getMessage().setBody(new ManualReviewRequired(
                    outcomeEventId("ManualReviewRequired", event.eventId(), exceptionCase.caseId(),
                            exceptionCase.version()),
                    event.eventId(), event.correlationId(), event.tenantId(), exceptionCase.caseId(),
                    value.assessment().rationale(), value.provenance().provider(),
                    value.provenance().model(),
                    value.provenance().instructionVersion()));
        };
    }

    private static String outcomeEventId(
            String outcomeType, String causedBy, String caseId, long caseVersion) {
        String identity = outcomeType + ":" + causedBy + ":" + caseId + ":" + caseVersion;
        return "evt-" + UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8));
    }
}
