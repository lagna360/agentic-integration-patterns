package dev.agenticintegrationpatterns.orderdesk.failure;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public final class FailureTaxonomyRoute extends RouteBuilder {
    private final FailureClassificationProcessor classification;

    public FailureTaxonomyRoute(FailureClassificationProcessor classification) {
        this.classification = classification;
    }

    @Override
    // tag::failure-route[]
    public void configure() {
        from("direct:classify-known-failure")
                .routeId("classify-known-failure")
                .process(classification)
                .choice()
                    .when(simple("${body.disposition} == 'REJECTED'"))
                        .to("seda:failure-rejected")
                    .when(simple("${body.disposition} == 'DENIED'"))
                        .to("seda:failure-denied")
                    .when(simple("${body.disposition} == 'RETRY_ELIGIBLE'"))
                        .to("seda:failure-retry-eligible")
                    .when(simple("${body.disposition} == 'STOPPED'"))
                        .to("seda:investigation-stopped")
                    .when(simple("${body.disposition} == 'BUSINESS_OUTCOME'"))
                        .to("seda:business-outcome")
                    .when(simple("${body.disposition} == 'RECONCILIATION_REQUIRED'"))
                        .to("seda:effect-reconciliation")
                    .otherwise()
                        .throwException(new IllegalStateException("unmapped failure disposition"))
                .end();
    }
    // end::failure-route[]
}
