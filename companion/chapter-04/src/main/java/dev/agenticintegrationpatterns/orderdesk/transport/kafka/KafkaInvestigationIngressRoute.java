package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.work.InvalidWorkEnvelopeException;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "orderdesk.kafka.enabled", havingValue = "true")
public final class KafkaInvestigationIngressRoute extends RouteBuilder {
    private final KafkaIngressPipeline pipeline;
    private final KafkaFailureDispositionProcessor failureDisposition;
    private final ManualKafkaCommitProcessor manualCommit;

    public KafkaInvestigationIngressRoute(
            KafkaIngressPipeline pipeline,
            KafkaFailureDispositionProcessor failureDisposition,
            ManualKafkaCommitProcessor manualCommit) {
        this.pipeline = pipeline;
        this.failureDisposition = failureDisposition;
        this.manualCommit = manualCommit;
    }

    @Override
    // tag::kafka-ingress-route[]
    public void configure() {
        onException(InvalidWorkEnvelopeException.class, InvalidKafkaRecordException.class)
            .handled(true)
            .process(failureDisposition)
            .process(manualCommit);

        from("kafka:{{orderdesk.kafka.command-topic}}"
                + "?brokers={{orderdesk.kafka.brokers}}"
                + "&groupId={{orderdesk.kafka.admission-group}}"
                + "&autoCommitEnable=false&allowManualCommit=true"
                + "&breakOnFirstError=true&isolationLevel=read_committed"
                + "&maxPollRecords=1")
            .routeId("kafka-investigation-admission")
            .process(pipeline)
            .process(manualCommit);
    }
    // end::kafka-ingress-route[]
}
