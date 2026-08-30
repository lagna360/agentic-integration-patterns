package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.CaseWork;
import dev.agenticintegrationpatterns.chapter04.model.InventoryShortfallDetected;
import dev.agenticintegrationpatterns.chapter04.model.OrderExceptionCase;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryCaseStore implements Processor {
    private final Map<String, OrderExceptionCase> cases = new ConcurrentHashMap<>();

    @Override
    public void process(Exchange exchange) {
        var event = exchange.getMessage().getBody(InventoryShortfallDetected.class);
        String key = event.tenantId() + ":" + event.orderId() + ":" + event.sku();
        var current = cases.compute(key, (ignored, existing) -> existing == null
                ? new OrderExceptionCase(caseIdFor(key), event.tenantId(),
                        event.orderId(), event.sku(), 1, "OPEN")
                : new OrderExceptionCase(existing.caseId(), existing.tenantId(),
                        existing.orderId(), existing.sku(),
                        existing.version() + 1, existing.status()));
        exchange.getMessage().setHeader("caseId", current.caseId());
        exchange.getMessage().setBody(new CaseWork(event, current));
    }

    private static String caseIdFor(String tenantScopedKey) {
        return "case-" + UUID.nameUUIDFromBytes(
                tenantScopedKey.getBytes(StandardCharsets.UTF_8));
    }

    public void reset() {
        cases.clear();
    }
}
