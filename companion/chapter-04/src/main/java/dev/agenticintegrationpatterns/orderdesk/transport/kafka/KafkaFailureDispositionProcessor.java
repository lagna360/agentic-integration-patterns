package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import dev.agenticintegrationpatterns.orderdesk.work.InvalidWorkEnvelopeException;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static dev.agenticintegrationpatterns.orderdesk.transport.kafka.KafkaRecordVerifier.FINGERPRINT;
import static dev.agenticintegrationpatterns.orderdesk.transport.kafka.KafkaRecordVerifier.POSITION;
import static dev.agenticintegrationpatterns.orderdesk.work.WorkEnvelopeAdmission.TRUSTED_CONTEXT_HEADER;

@Component
public final class KafkaFailureDispositionProcessor implements Processor {
    private final JdbcCommandInbox inbox;
    private final String authenticatedProducerRef;

    public KafkaFailureDispositionProcessor(
            JdbcCommandInbox inbox,
            @Value("${orderdesk.kafka.authenticated-producer-ref}") String authenticatedProducerRef) {
        this.inbox = inbox;
        this.authenticatedProducerRef = authenticatedProducerRef;
    }

    @Override
    public void process(Exchange exchange) {
        var message = exchange.getMessage();
        var command = message.getBody() instanceof InvestigateOrderException value ? value : null;
        var caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String reason = caught instanceof InvalidWorkEnvelopeException invalid
                ? "WORK_ENVELOPE_" + invalid.violation().name()
                : "TRANSPORT_MAPPING_REJECTED";
        inbox.quarantineFailure(
                command == null ? null : command.tenantId(),
                authenticatedProducerRef,
                command == null ? null : command.commandId(),
                message.getHeader(FINGERPRINT, String.class),
                reason,
                message.getHeader(POSITION, KafkaTransportPosition.class));
        message.setBody(new RejectedIngress(reason));
        message.removeHeader(TRUSTED_CONTEXT_HEADER);
        message.setHeader("ingressDisposition", "QUARANTINED");
    }

    public record RejectedIngress(String reason) {}
}
