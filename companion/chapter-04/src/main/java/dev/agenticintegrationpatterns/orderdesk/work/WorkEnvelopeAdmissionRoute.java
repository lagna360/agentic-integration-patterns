package dev.agenticintegrationpatterns.orderdesk.work;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public final class WorkEnvelopeAdmissionRoute extends RouteBuilder {
    private final WorkEnvelopeDecoder decoder;
    private final WorkEnvelopeAdmission admission;

    @Value("${orderdesk.work.accepted-uri:seda:admitted-investigation}")
    private String acceptedUri;

    @Value("${orderdesk.work.rejected-uri:seda:rejected-investigation}")
    private String rejectedUri;

    public WorkEnvelopeAdmissionRoute(WorkEnvelopeDecoder decoder, WorkEnvelopeAdmission admission) {
        this.decoder = decoder;
        this.admission = admission;
    }

    @Override
    public void configure() {
        onException(InvalidWorkEnvelopeException.class)
                .handled(true)
                .process(exchange -> {
                    var failure = exchange.getProperty(
                            Exchange.EXCEPTION_CAUGHT, InvalidWorkEnvelopeException.class);
                    var command = exchange.getMessage().getBody() instanceof InvestigateOrderException value
                            ? value : null;
                    var trusted = exchange.getMessage().getHeader(
                            WorkEnvelopeAdmission.TRUSTED_CONTEXT_HEADER, TrustedAdmissionContext.class);
                    exchange.getMessage().getHeaders().clear();
                    exchange.getMessage().setBody(new RejectedInvestigation(
                            failure.violation(),
                            command == null ? null : command.commandId(),
                            command == null ? null : command.correlationId(),
                            command == null ? null : command.tenantId(),
                            trusted == null ? null : trusted.receivedAt(),
                            failure.getMessage()));
                })
                .to(rejectedUri);

        // tag::route[]
        from("direct:admit-investigation")
                .routeId("admit-investigation-work")
                .process(decoder)
                .process(admission)
                .to(acceptedUri);
        // end::route[]
    }
}
