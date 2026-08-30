package dev.agenticintegrationpatterns.orderdesk.effect;

public interface InventoryReservationClient {
    InvocationResult reserve(ReservationRequest request);

    TargetObservation findByIdempotencyKey(
            String tenantId, String targetIdempotencyKey);

    record ReservationRequest(
            String tenantId,
            String effectId,
            String attemptId,
            String targetIdempotencyKey,
            String warehouseId,
            String sku,
            int quantity) {
    }

    record InvocationResult(
            Outcome outcome,
            String targetReference,
            String evidenceRef) {
        public InvocationResult {
            if (outcome == null) {
                throw new IllegalArgumentException("outcome is required");
            }
            if (outcome == Outcome.UNKNOWN) {
                throw new IllegalArgumentException(
                        "an invocation reports uncertainty through ExternalOutcomeUnknownException");
            }
            ReserveInventoryEffect.requireOptionalText(
                    targetReference, "targetReference", 600);
            if (outcome == Outcome.ACCEPTED || outcome == Outcome.SUCCEEDED) {
                ReserveInventoryEffect.requireText(
                        targetReference, "targetReference", 600);
            }
            ReserveInventoryEffect.requireText(evidenceRef, "evidenceRef", 600);
        }
    }

    record TargetObservation(
            Outcome outcome,
            String targetReference,
            String evidenceRef) {
        public TargetObservation {
            if (outcome == null) {
                throw new IllegalArgumentException("outcome is required");
            }
            ReserveInventoryEffect.requireOptionalText(
                    targetReference, "targetReference", 600);
            ReserveInventoryEffect.requireText(evidenceRef, "evidenceRef", 600);
            if (outcome == Outcome.ACCEPTED || outcome == Outcome.SUCCEEDED) {
                ReserveInventoryEffect.requireText(
                        targetReference, "targetReference", 600);
            }
        }
    }

    enum Outcome {
        ACCEPTED,
        SUCCEEDED,
        FAILED_CONFIRMED,
        UNKNOWN
    }

    final class ExternalOutcomeUnknownException extends RuntimeException {
        private final String evidenceRef;

        public ExternalOutcomeUnknownException(String message, String evidenceRef) {
            super(message);
            ReserveInventoryEffect.requireText(evidenceRef, "evidenceRef", 600);
            this.evidenceRef = evidenceRef;
        }

        public String evidenceRef() {
            return evidenceRef;
        }
    }
}
