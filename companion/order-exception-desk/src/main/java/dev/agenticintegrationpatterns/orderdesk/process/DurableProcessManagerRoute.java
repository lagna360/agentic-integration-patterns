package dev.agenticintegrationpatterns.orderdesk.process;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class DurableProcessManagerRoute extends RouteBuilder {
    private final JdbcDurableProcessManager manager;

    public DurableProcessManagerRoute(JdbcDurableProcessManager manager) {
        this.manager = manager;
    }

    @Override
    public void configure() {
        // tag::process-manager-route[]
        from("direct:start-investigation-run")
                .routeId("start-investigation-run")
                .bean(manager, "start");

        // Internal seam: only a server-owned dispatcher can attach a RunLease.
        from("direct:accept-claimed-evidence")
                .routeId("accept-claimed-evidence")
                .process(exchange -> {
                    var input = exchange.getMessage().getBody(LeasedEvidenceClosure.class);
                    exchange.getMessage().setBody(
                            manager.applyEvidence(input.lease(), input.evidence()));
                });
        // end::process-manager-route[]
    }
}
