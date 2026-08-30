package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static dev.agenticintegrationpatterns.orderdesk.transport.kafka.KafkaRecordVerifier.FINGERPRINT;
import static dev.agenticintegrationpatterns.orderdesk.transport.kafka.KafkaRecordVerifier.POSITION;

@Component
public final class KafkaInboxProcessor implements Processor {
    private final JdbcCommandInbox inbox;
    private final String authenticatedProducerRef;

    public KafkaInboxProcessor(
            JdbcCommandInbox inbox,
            @Value("${orderdesk.kafka.authenticated-producer-ref}") String authenticatedProducerRef) {
        this.inbox = inbox;
        this.authenticatedProducerRef = authenticatedProducerRef;
    }

    @Override
    public void process(Exchange exchange) {
        var message = exchange.getMessage();
        var receipt = inbox.admit(
                message.getBody(AdmittedInvestigation.class),
                authenticatedProducerRef,
                message.getHeader(FINGERPRINT, String.class),
                message.getHeader(POSITION, KafkaTransportPosition.class));
        message.setHeader("ingressDisposition", receipt.disposition());
        message.setHeader("runId", receipt.runId());
    }
}
