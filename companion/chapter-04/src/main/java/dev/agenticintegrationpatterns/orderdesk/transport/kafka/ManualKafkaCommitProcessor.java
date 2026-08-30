package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.springframework.stereotype.Component;

@Component
public final class ManualKafkaCommitProcessor implements Processor {
    @Override
    // tag::manual-commit[]
    public void process(Exchange exchange) {
        var commit = exchange.getMessage().getHeader(
                KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (commit == null) {
            throw new IllegalStateException("Kafka manual commit handle is missing");
        }
        commit.commit();
    }
    // end::manual-commit[]
}
