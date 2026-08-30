package dev.agenticintegrationpatterns.orderdesk.capability;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public final class FixtureInventoryAvailabilityClient implements InventoryAvailabilityClient {
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public InventoryAvailabilityObservation read(
            String trustedTenantId,
            InventoryAvailabilityArguments arguments) {
        calls.incrementAndGet();
        if (!"tenant-ca".equals(trustedTenantId)
                || !"camera-battery-x2".equals(arguments.sku())
                || !"yyz-01".equals(arguments.locationId())) {
            throw new IllegalStateException("Fixture inventory record is unavailable");
        }
        return new InventoryAvailabilityObservation(
                trustedTenantId, arguments.sku(), arguments.locationId(), 0, "740",
                Instant.parse("2026-08-24T06:13:12Z"),
                Instant.parse("2026-08-24T06:18:12Z"));
    }

    public int calls() {
        return calls.get();
    }

    public void reset() {
        calls.set(0);
    }
}
