package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.InventoryShortfallDetected;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.TypeConversionException;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
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
            var body = exchange.getMessage().getBody(String.class);
            if (body == null) {
                throw new MalformedEventException(
                        "Payload is not a valid InventoryShortfallDetected event",
                        new IllegalArgumentException("Message body is null"));
            }
            var event = objectMapper.readValue(
                    body,
                    InventoryShortfallDetected.class);
            exchange.getMessage().setBody(event);
            exchange.getMessage().setHeader("eventId", event.eventId());
            exchange.getMessage().setHeader("correlationId", event.correlationId());
            exchange.getMessage().setHeader("tenantId", event.tenantId());
        } catch (JacksonException | TypeConversionException malformed) {
            throw new MalformedEventException("Payload is not a valid InventoryShortfallDetected event", malformed);
        }
    }
}
