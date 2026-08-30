package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.CaseWork;
import dev.agenticintegrationpatterns.orderdesk.model.EvidenceReference;
import dev.agenticintegrationpatterns.orderdesk.model.InventoryObservation;
import dev.agenticintegrationpatterns.orderdesk.model.OrderContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

@Component
public class FixtureOrderContextProvider implements Processor {
    @Override
    public void process(Exchange exchange) {
        var work = exchange.getMessage().getBody(CaseWork.class);
        String expected = work.event().correlationId();
        String actual = exchange.getMessage().getHeader("correlationId", String.class);
        if (!expected.equals(actual)) {
            throw new InvalidEventException("Correlation identity changed during enrichment");
        }

        Instant observedAt = Instant.parse("2026-08-24T06:13:12Z");
        String reference = "inventory://yyz-02/camera-battery-x2@741";
        exchange.getMessage().setBody(new OrderContext(
                work.event().orderId(),
                List.of(new InventoryObservation("yyz-02", 2, observedAt, reference)),
                List.of(new EvidenceReference(reference, observedAt, true))));
    }
}
