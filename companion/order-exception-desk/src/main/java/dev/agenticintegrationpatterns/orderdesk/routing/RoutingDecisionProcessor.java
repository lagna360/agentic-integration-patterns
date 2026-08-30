package dev.agenticintegrationpatterns.orderdesk.routing;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public final class RoutingDecisionProcessor implements Processor {
    private final RoutingDecisionService service;

    public RoutingDecisionProcessor(RoutingDecisionService service) {
        this.service = service;
    }

    @Override
    public void process(Exchange exchange) {
        var request = exchange.getMessage().getBody(InvestigationRoutingRequest.class);
        exchange.getMessage().removeHeaders("*");
        exchange.getMessage().setBody(service.decide(request));
    }
}
