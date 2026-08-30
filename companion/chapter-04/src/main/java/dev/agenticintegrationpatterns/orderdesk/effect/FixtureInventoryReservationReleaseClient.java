package dev.agenticintegrationpatterns.orderdesk.effect;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static dev.agenticintegrationpatterns.orderdesk.effect.InventoryReservationClient.Outcome.*;

/** Deterministic target double; a release is a new external effect, not an in-memory undo. */
@Component
public final class FixtureInventoryReservationReleaseClient
        implements InventoryReservationReleaseClient {
    private final AtomicReference<Mode> next = new AtomicReference<>(Mode.SUCCEED);
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public InventoryReservationClient.InvocationResult release(ReleaseRequest request) {
        calls.incrementAndGet();
        var mode = next.getAndSet(Mode.SUCCEED);
        if (mode == Mode.LOSE_REPLY) {
            throw new InventoryReservationClient.ExternalOutcomeUnknownException(
                    "reservation-release reply was lost",
                    "warehouse-release-timeout:" + request.attemptId());
        }
        var outcome = mode == Mode.FAIL_CONFIRMED ? FAILED_CONFIRMED : SUCCEEDED;
        String reference = "reservation-release/" + request.idempotencyKey();
        return new InventoryReservationClient.InvocationResult(
                outcome, reference, "warehouse-release-receipt:" + reference);
    }

    public void nextResultWillBeConfirmedFailure() {
        next.set(Mode.FAIL_CONFIRMED);
    }

    public void nextReplyWillBeUnknown() {
        next.set(Mode.LOSE_REPLY);
    }

    public int callCount() {
        return calls.get();
    }

    public void reset() {
        next.set(Mode.SUCCEED);
        calls.set(0);
    }

    private enum Mode { SUCCEED, FAIL_CONFIRMED, LOSE_REPLY }
}
