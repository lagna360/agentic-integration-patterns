package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public final class KafkaRecordVerifier implements Processor {
    public static final String FINGERPRINT = "orderdeskPayloadFingerprint";
    public static final String POSITION = "orderdeskKafkaPosition";
    private final ObjectMapper mapper;

    public KafkaRecordVerifier(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void process(Exchange exchange) {
        var message = exchange.getMessage();
        var command = message.getBody(InvestigateOrderException.class);
        String key = message.getHeader(KafkaConstants.KEY, String.class);
        TenantCaseKey.requireMatch(key, command);
        message.setHeader(FINGERPRINT, PayloadFingerprint.canonical(mapper, command));
        message.setHeader(POSITION, new KafkaTransportPosition(
                message.getHeader(KafkaConstants.TOPIC, String.class),
                message.getHeader(KafkaConstants.PARTITION, Integer.class),
                message.getHeader(KafkaConstants.OFFSET, Long.class), key));
    }
}
