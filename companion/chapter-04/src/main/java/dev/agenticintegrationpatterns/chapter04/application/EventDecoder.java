package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.InventoryShortfallDetected;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class EventDecoder implements Processor {
    private final ObjectMapper objectMapper;

    public EventDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void process(Exchange exchange) {
        try {
            var event = objectMapper.readValue(
                    exchange.getMessage().getBody(String.class),
                    InventoryShortfallDetected.class);
            exchange.getMessage().setBody(event);
            exchange.getMessage().setHeader("eventId", event.eventId());
            exchange.getMessage().setHeader("correlationId", event.correlationId());
            exchange.getMessage().setHeader("tenantId", event.tenantId());
        } catch (Exception malformed) {
            throw new MalformedEventException("Payload is not a valid InventoryShortfallDetected event", malformed);
        }
    }
}
