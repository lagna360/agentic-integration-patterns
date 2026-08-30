package dev.agenticintegrationpatterns.orderdesk.context;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public final class ContextResolutionProcessor implements Processor {
    private final ContextResolutionService service;

    public ContextResolutionProcessor(ContextResolutionService service) {
        this.service = service;
    }

    @Override
    public void process(Exchange exchange) {
        var request = exchange.getMessage().getBody(ContextResolutionRequest.class);
        exchange.getMessage().setBody(service.resolve(request));
    }
}
