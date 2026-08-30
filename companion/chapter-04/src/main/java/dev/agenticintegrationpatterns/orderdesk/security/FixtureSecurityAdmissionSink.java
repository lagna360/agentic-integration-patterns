package dev.agenticintegrationpatterns.orderdesk.security;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/** Counts handoffs to the existing effect boundary; it is not a target adapter. */
@Component
public class FixtureSecurityAdmissionSink {
    private final AtomicInteger handoffs = new AtomicInteger();

    public SecurityDecision accept(SecurityDecision decision) {
        handoffs.incrementAndGet();
        return decision;
    }

    public int handoffCount() {
        return handoffs.get();
    }

    public void reset() {
        handoffs.set(0);
    }
}
