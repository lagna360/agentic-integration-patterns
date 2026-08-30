package dev.agenticintegrationpatterns.orderdesk.routing;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import static dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision.Target.*;

@Component
public final class InvestigationRoutingRoute extends RouteBuilder {
    private final RoutingDecisionProcessor processor;

    public InvestigationRoutingRoute(RoutingDecisionProcessor processor) {
        this.processor = processor;
    }

    @Override
    // tag::fixed-routing-route[]
    public void configure() {
        from("direct:route-investigation")
                .routeId("route-validated-investigation")
                .process(processor)
                .choice()
                    .when(exchange -> exchange.getMessage()
                            .getBody(RoutingDecision.class).target() == INVENTORY_FOLLOW_UP)
                        .to("seda:inventory-follow-up")
                    .when(exchange -> exchange.getMessage()
                            .getBody(RoutingDecision.class).target() == ORDER_FOLLOW_UP)
                        .to("seda:order-follow-up")
                    .when(exchange -> exchange.getMessage()
                            .getBody(RoutingDecision.class).target() == READY_FOR_ASSESSMENT)
                        .to("seda:ready-for-assessment")
                    .when(exchange -> exchange.getMessage()
                            .getBody(RoutingDecision.class).target() == INVESTIGATION_STOPPED)
                        .to("seda:investigation-stopped")
                    .otherwise()
                        .to("seda:manual-review");
    }
    // end::fixed-routing-route[]
}
