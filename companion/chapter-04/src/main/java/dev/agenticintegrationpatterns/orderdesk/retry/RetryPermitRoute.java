package dev.agenticintegrationpatterns.orderdesk.retry;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public final class RetryPermitRoute extends RouteBuilder {
    private final RetryAttemptAdmission admission;

    public RetryPermitRoute(RetryAttemptAdmission admission) {
        this.admission = admission;
    }

    @Override
    public void configure() {
        // This route owns no retry. One delivery can yield at most one durable permit.
        errorHandler(noErrorHandler());
        // tag::single-permit-camel-route[]
        from("direct:claim-due-retry")
                .routeId("claim-due-retry")
                .bean(admission, "admit");
        // end::single-permit-camel-route[]
    }
}
