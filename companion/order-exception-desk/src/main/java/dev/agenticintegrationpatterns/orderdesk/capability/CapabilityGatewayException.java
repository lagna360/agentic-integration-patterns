package dev.agenticintegrationpatterns.orderdesk.capability;

public final class CapabilityGatewayException extends RuntimeException {
    private final Reason reason;

    public CapabilityGatewayException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

    public enum Reason {
        INVALID_INTENT,
        UNKNOWN_CAPABILITY,
        CAPABILITY_DENIED,
        DEADLINE_EXCEEDED,
        TOOL_BUDGET_EXHAUSTED,
        ARGUMENT_SCHEMA_VIOLATION,
        OBJECT_SCOPE_DENIED,
        UPSTREAM_UNAVAILABLE,
        RESULT_INVALID
    }
}
