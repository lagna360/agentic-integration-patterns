package dev.agenticintegrationpatterns.orderdesk.effect;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static dev.agenticintegrationpatterns.orderdesk.effect.InventoryReservationClient.Outcome.*;

/** Deterministic teaching adapter; not a production warehouse integration. */
@Component
public final class FixtureInventoryReservationClient
        implements InventoryReservationClient {
    private final Map<String, StoredReservation> reservations = new ConcurrentHashMap<>();
    private final AtomicReference<Mode> nextMode = new AtomicReference<>(Mode.SUCCEED);
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public InvocationResult reserve(ReservationRequest request) {
        calls.incrementAndGet();
        String mapKey = request.tenantId() + "\u001f" + request.targetIdempotencyKey();
        String requestSha = requestSha(request);
        var existing = reservations.get(mapKey);
        if (existing != null) {
            if (!existing.requestSha().equals(requestSha)) {
                throw new IllegalArgumentException(
                        "target idempotency key reused with different parameters");
            }
            return new InvocationResult(
                    existing.outcome(), existing.targetReference(),
                    "target-replay:" + existing.targetReference());
        }

        String targetReference = request.effectId().equals("effect-reserve-13")
                ? "rsv-8842" : "reservation/" + request.targetIdempotencyKey();
        return switch (nextMode.getAndSet(Mode.SUCCEED)) {
            case SUCCEED -> {
                reservations.put(mapKey,
                        new StoredReservation(requestSha, SUCCEEDED, targetReference));
                yield new InvocationResult(
                        SUCCEEDED, targetReference, "target-receipt:" + targetReference);
            }
            case ACCEPT_ASYNC -> {
                reservations.put(mapKey,
                        new StoredReservation(requestSha, ACCEPTED, targetReference));
                yield new InvocationResult(
                        ACCEPTED, targetReference, "target-accepted:" + targetReference);
            }
            case FAIL_CONFIRMED -> {
                reservations.put(mapKey,
                        new StoredReservation(requestSha, FAILED_CONFIRMED, targetReference));
                yield new InvocationResult(
                        FAILED_CONFIRMED, targetReference,
                        "target-rejection:" + targetReference);
            }
            case COMMIT_THEN_LOSE_REPLY -> {
                reservations.put(mapKey,
                        new StoredReservation(requestSha, SUCCEEDED, targetReference));
                throw new ExternalOutcomeUnknownException(
                        "warehouse reply was lost after the commit boundary",
                        "timeout-after-send:" + request.attemptId());
            }
        };
    }

    @Override
    public TargetObservation findByIdempotencyKey(
            String tenantId, String targetIdempotencyKey) {
        var stored = reservations.get(tenantId + "\u001f" + targetIdempotencyKey);
        if (stored == null) {
            return new TargetObservation(
                    UNKNOWN, null, "warehouse-query:no-authoritative-answer");
        }
        return new TargetObservation(
                stored.outcome(), stored.targetReference(),
                "warehouse-query:" + stored.targetReference());
    }

    public void nextReplyWillBeLostAfterCommit() {
        nextMode.set(Mode.COMMIT_THEN_LOSE_REPLY);
    }

    public void nextResultWillBeAcceptedAsynchronously() {
        nextMode.set(Mode.ACCEPT_ASYNC);
    }

    public void nextResultWillBeConfirmedNotApplied() {
        nextMode.set(Mode.FAIL_CONFIRMED);
    }

    public void resolveAcceptedAsSucceeded(String tenantId, String idempotencyKey) {
        reservations.computeIfPresent(tenantId + "\u001f" + idempotencyKey,
                (key, value) -> new StoredReservation(
                        value.requestSha(), SUCCEEDED, value.targetReference()));
    }

    public int callCount() {
        return calls.get();
    }

    public void reset() {
        reservations.clear();
        calls.set(0);
        nextMode.set(Mode.SUCCEED);
    }

    private static String requestSha(ReservationRequest request) {
        try {
            String canonical = request.tenantId().length() + ":" + request.tenantId()
                    + request.warehouseId().length() + ":" + request.warehouseId()
                    + request.sku().length() + ":" + request.sku()
                    + request.quantity();
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private enum Mode {
        SUCCEED,
        ACCEPT_ASYNC,
        FAIL_CONFIRMED,
        COMMIT_THEN_LOSE_REPLY
    }

    private record StoredReservation(
            String requestSha,
            Outcome outcome,
            String targetReference) { }
}
