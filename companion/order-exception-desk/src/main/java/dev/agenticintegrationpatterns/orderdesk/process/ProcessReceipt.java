package dev.agenticintegrationpatterns.orderdesk.process;

public record ProcessReceipt(Disposition disposition, String runId, RunState state, long version) {
    public enum Disposition {
        APPLIED,
        DUPLICATE_SAME,
        MESSAGE_ID_COLLISION,
        LATE,
        OUT_OF_ORDER,
        INVALID_EVIDENCE,
        DEADLINE_EXCEEDED,
        STALE_FENCE,
        UNKNOWN_RUN
    }
}
