package dev.agenticintegrationpatterns.orderdesk.security;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class RouteSecurityRoute extends RouteBuilder {
    private final JdbcRouteSecurityGate gate;
    private final FixtureProtectedContextProvider contextProvider;
    private final FixtureSecurityAdmissionSink sink;

    public RouteSecurityRoute(
            JdbcRouteSecurityGate gate, FixtureProtectedContextProvider contextProvider,
            FixtureSecurityAdmissionSink sink) {
        this.gate = gate;
        this.contextProvider = contextProvider;
        this.sink = sink;
    }

    @Override
    public void configure() {
        // tag::route-security-camel[]
        from("direct:route-security-pregate")
                .routeId("route-security-pregate")
                .bean(contextProvider, "attach")
                .bean(gate, "authorize")
                .choice()
                    .when(simple("${body.outcome} == 'ALLOW'"))
                        .bean(sink, "accept")
                    .otherwise()
                        .stop()
                .end();
        // end::route-security-camel[]
    }
}
