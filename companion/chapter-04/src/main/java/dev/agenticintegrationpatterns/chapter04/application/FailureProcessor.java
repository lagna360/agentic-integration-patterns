package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.FailureKind;
import dev.agenticintegrationpatterns.chapter04.model.ProcessingFailure;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public class FailureProcessor {
    public Processor invalidMessage() {
        return asFailure(FailureKind.INVALID_MESSAGE);
    }

    public Processor assessmentFailure() {
        return exchange -> {
            Exception caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
            FailureKind kind = caught instanceof InvalidAssessmentException
                    ? FailureKind.INVALID_ASSESSMENT
                    : FailureKind.ASSESSMENT_UNAVAILABLE;
            setFailure(exchange, kind, caught);
        };
    }

    private Processor asFailure(FailureKind kind) {
        return exchange -> setFailure(
                exchange,
                kind,
                exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class));
    }

    private void setFailure(Exchange exchange, FailureKind kind, Exception caught) {
        String eventId = exchange.getMessage().getHeader("eventId", String.class);
        String correlationId = exchange.getMessage().getHeader("correlationId", String.class);
        String tenantId = exchange.getMessage().getHeader("tenantId", String.class);
        String caseId = exchange.getMessage().getHeader("caseId", String.class);
        String detail = caught == null ? kind.name() : caught.getMessage();
        exchange.getMessage().setBody(new ProcessingFailure(
                kind, eventId, correlationId, tenantId, caseId, detail));
    }
}
