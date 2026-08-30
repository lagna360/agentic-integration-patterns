package dev.agenticintegrationpatterns.orderdesk.capability;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public final class CapabilityInvocationProcessor implements Processor {
    private final GovernedCapabilityGateway gateway;

    public CapabilityInvocationProcessor(GovernedCapabilityGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public void process(Exchange exchange) {
        var request = exchange.getMessage().getBody(CapabilityInvocationRequest.class);
        exchange.getMessage().removeHeaders("*");
        exchange.getMessage().setBody(gateway.invoke(request));
    }
}
