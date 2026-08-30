package dev.agenticintegrationpatterns.orderdesk.work;

public final class InvalidWorkEnvelopeException extends RuntimeException {
    private final Violation violation;

    public InvalidWorkEnvelopeException(Violation violation, String message) {
        super(message);
        this.violation = violation;
    }

    public Violation violation() {
        return violation;
    }

    public enum Violation {
        MALFORMED,
        UNSUPPORTED_CONTRACT,
        INVALID_IDENTITY,
        IDENTITY_MISMATCH,
        EXPIRED,
        INVALID_LIMITS,
        CAPABILITY_DENIED,
        INVALID_EVIDENCE,
        UNKNOWN_CONFIGURATION,
        UNKNOWN_REPLY_CONTRACT
    }
}
