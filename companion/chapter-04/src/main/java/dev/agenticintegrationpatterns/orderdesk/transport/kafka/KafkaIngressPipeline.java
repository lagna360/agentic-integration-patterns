package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.work.WorkEnvelopeAdmission;
import dev.agenticintegrationpatterns.orderdesk.work.WorkEnvelopeDecoder;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public final class KafkaIngressPipeline implements Processor {
    private final KafkaTransportMetadataProcessor metadata;
    private final KafkaTrustedContextProcessor trustedContext;
    private final WorkEnvelopeDecoder decoder;
    private final KafkaRecordVerifier recordVerifier;
    private final WorkEnvelopeAdmission admission;
    private final KafkaInboxProcessor inbox;
    private final AfterInboxHook afterInbox;

    public KafkaIngressPipeline(
            KafkaTransportMetadataProcessor metadata,
            KafkaTrustedContextProcessor trustedContext,
            WorkEnvelopeDecoder decoder,
            KafkaRecordVerifier recordVerifier,
            WorkEnvelopeAdmission admission,
            KafkaInboxProcessor inbox,
            AfterInboxHook afterInbox) {
        this.metadata = metadata;
        this.trustedContext = trustedContext;
        this.decoder = decoder;
        this.recordVerifier = recordVerifier;
        this.admission = admission;
        this.inbox = inbox;
        this.afterInbox = afterInbox;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        metadata.process(exchange);
        trustedContext.process(exchange);
        decoder.process(exchange);
        recordVerifier.process(exchange);
        admission.process(exchange);
        inbox.process(exchange);
        afterInbox.afterInbox(exchange);
    }
}
