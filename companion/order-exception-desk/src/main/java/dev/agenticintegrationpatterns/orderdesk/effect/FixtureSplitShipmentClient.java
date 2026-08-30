package dev.agenticintegrationpatterns.orderdesk.effect;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static dev.agenticintegrationpatterns.orderdesk.effect.InventoryReservationClient.Outcome.*;

/** Deterministic target double for the Chapter 16 forced later-step failure. */
@Component
public final class FixtureSplitShipmentClient implements SplitShipmentClient {
    private final AtomicReference<Mode> next = new AtomicReference<>(Mode.SUCCEED);
    private final AtomicInteger calls = new AtomicInteger();
    private volatile long lastExpectedOrderVersion;

    @Override
    public InventoryReservationClient.InvocationResult create(CreateRequest request) {
        calls.incrementAndGet();
        lastExpectedOrderVersion = request.expectedOrderVersion();
        var mode = next.getAndSet(Mode.SUCCEED);
        if (mode == Mode.LOSE_REPLY) {
            throw new InventoryReservationClient.ExternalOutcomeUnknownException(
                    "split-shipment reply was lost",
                    "fulfillment-timeout:" + request.attemptId());
        }
        var outcome = switch (mode) {
            case SUCCEED -> SUCCEEDED;
            case FAIL_CONFIRMED -> FAILED_CONFIRMED;
            case ACCEPT -> ACCEPTED;
            case LOSE_REPLY -> throw new IllegalStateException("handled above");
        };
        String reference = "split-shipment/" + request.idempotencyKey();
        String evidence = outcome == FAILED_CONFIRMED
                ? "fulfillment-rejection:ORDER_VERSION_MISMATCH:expected="
                    + request.expectedOrderVersion()
                : "fulfillment-receipt:" + reference;
        return new InventoryReservationClient.InvocationResult(outcome, reference, evidence);
    }

    public void nextResultWillBeConfirmedFailure() {
        next.set(Mode.FAIL_CONFIRMED);
    }

    public void nextResultWillBeAccepted() {
        next.set(Mode.ACCEPT);
    }

    public void nextReplyWillBeUnknown() {
        next.set(Mode.LOSE_REPLY);
    }

    public int callCount() {
        return calls.get();
    }

    public long lastExpectedOrderVersion() {
        return lastExpectedOrderVersion;
    }

    public void reset() {
        next.set(Mode.SUCCEED);
        calls.set(0);
        lastExpectedOrderVersion = 0;
    }

    private enum Mode { SUCCEED, FAIL_CONFIRMED, ACCEPT, LOSE_REPLY }
}
