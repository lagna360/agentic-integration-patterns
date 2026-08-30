package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.work.TrustedAdmissionContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.work.WorkEnvelopeAdmission.TRUSTED_CONTEXT_HEADER;

@Component
public final class KafkaTrustedContextProcessor implements Processor {
    private final String producerRef;
    private final Clock clock;

    public KafkaTrustedContextProcessor(
            @Value("${orderdesk.kafka.authenticated-producer-ref}") String producerRef,
            Clock clock) {
        this.producerRef = producerRef;
        this.clock = clock;
    }

    @Override
    public void process(Exchange exchange) {
        exchange.getMessage().setHeader(TRUSTED_CONTEXT_HEADER,
                new TrustedAdmissionContext("tenant-ca", producerRef,
                        Set.of("principal:order-ops-ca"), clock.instant()));
    }
}
