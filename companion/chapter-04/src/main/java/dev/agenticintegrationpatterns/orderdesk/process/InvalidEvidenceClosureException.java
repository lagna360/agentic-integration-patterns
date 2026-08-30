package dev.agenticintegrationpatterns.orderdesk.process;

public final class InvalidEvidenceClosureException extends RuntimeException {
    private final Reason reason;

    public InvalidEvidenceClosureException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

    public enum Reason {
        EXPECTED_WORK_MISMATCH,
        REQUIRED_WORK_POLICY_CONTRADICTION
    }
}
