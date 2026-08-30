package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.InventoryShortfallDetected;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public class EventValidator implements Processor {
    @Override
    public void process(Exchange exchange) {
        var event = exchange.getMessage().getBody(InventoryShortfallDetected.class);
        require(event.schemaVersion() == 1, "Unsupported schemaVersion");
        require("InventoryShortfallDetected".equals(event.type()), "Unexpected event type");
        require(text(event.eventId()) && text(event.correlationId()) && text(event.tenantId()),
                "Missing event identity or tenancy");
        require(text(event.orderId()) && text(event.sku()) && text(event.warehouseId()),
                "Missing order identity");
        require(event.occurredAt() != null, "Missing occurredAt");
        require(event.requestedQuantity() > 0, "requestedQuantity must be positive");
        require(event.availableQuantity() >= 0, "availableQuantity must not be negative");
        require(event.availableQuantity() < event.requestedQuantity(), "Event does not describe a shortfall");
    }

    private static boolean text(String value) {
        return value != null && !value.isBlank();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new InvalidEventException(message);
        }
    }
}
