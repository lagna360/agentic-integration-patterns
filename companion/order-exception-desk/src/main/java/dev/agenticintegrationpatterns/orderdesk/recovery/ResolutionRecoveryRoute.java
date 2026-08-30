package dev.agenticintegrationpatterns.orderdesk.recovery;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class ResolutionRecoveryRoute extends RouteBuilder {
    private final JdbcResolutionRecoveryManager manager;

    public ResolutionRecoveryRoute(JdbcResolutionRecoveryManager manager) {
        this.manager = manager;
    }

    @Override
    public void configure() {
        // tag::resolution-recovery-route[]
        from("direct:observe-resolution-effect")
                .routeId("observe-resolution-effect")
                .bean(manager, "observe");

        from("direct:select-reservation-release-recovery")
                .routeId("select-reservation-release-recovery")
                .bean(manager, "selectCompensation");
        // end::resolution-recovery-route[]
    }
}
