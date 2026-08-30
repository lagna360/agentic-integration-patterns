package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

import static dev.agenticintegrationpatterns.orderdesk.transport.kafka.KafkaRecordVerifier.FINGERPRINT;
import static dev.agenticintegrationpatterns.orderdesk.transport.kafka.KafkaRecordVerifier.POSITION;

@Component
public final class KafkaTransportMetadataProcessor implements Processor {
    @Override
    public void process(Exchange exchange) {
        var message = exchange.getMessage();
        String payload = message.getBody(String.class);
        String topic = message.getHeader(KafkaConstants.TOPIC, String.class);
        Integer partition = message.getHeader(KafkaConstants.PARTITION, Integer.class);
        Long offset = message.getHeader(KafkaConstants.OFFSET, Long.class);
        String key = message.getHeader(KafkaConstants.KEY, String.class);
        if (payload == null || topic == null || partition == null || offset == null) {
            throw new InvalidKafkaRecordException("Required Kafka transport metadata is missing");
        }
        message.setHeader(FINGERPRINT, PayloadFingerprint.raw(payload));
        message.setHeader(POSITION, new KafkaTransportPosition(topic, partition, offset, key));
    }
}
