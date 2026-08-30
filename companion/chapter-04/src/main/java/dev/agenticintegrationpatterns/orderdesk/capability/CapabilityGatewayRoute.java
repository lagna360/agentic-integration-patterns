package dev.agenticintegrationpatterns.orderdesk.capability;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public final class CapabilityGatewayRoute extends RouteBuilder {
    private final CapabilityInvocationProcessor invocation;
    private final CapabilityFailureProcessor failure;

    public CapabilityGatewayRoute(
            CapabilityInvocationProcessor invocation,
            CapabilityFailureProcessor failure) {
        this.invocation = invocation;
        this.failure = failure;
    }

    @Override
    // tag::capability-route[]
    public void configure() {
        onException(CapabilityGatewayException.class)
                .handled(true)
                .process(failure)
                .to("seda:capability-unavailable");

        from("direct:invoke-capability")
                .routeId("invoke-governed-capability")
                .process(invocation)
                .to("seda:capability-evidence");
    }
    // end::capability-route[]
}
