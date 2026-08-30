package dev.agenticintegrationpatterns.orderdesk.recovery;

public record ResolutionReceipt(
        Disposition disposition,
        String tenantId,
        String planId,
        State state,
        long version) {
    public enum Disposition {
        CREATED, APPLIED, DUPLICATE_SAME, IDENTITY_COLLISION, STALE_OBSERVATION, OUT_OF_ORDER
    }

    public enum State {
        FORWARD_RUNNING,
        OBSERVATION_REQUIRED,
        RECOVERY_DECISION_REQUIRED,
        COMPENSATION_PENDING,
        COMPENSATED,
        COMPLETED,
        MANUAL_RECOVERY
    }
}
