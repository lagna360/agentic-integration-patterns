package dev.agenticintegrationpatterns.orderdesk.context;

public final class ContextResolutionException extends RuntimeException {
    private final Reason reason;
    private final String reference;

    public ContextResolutionException(Reason reason, String reference, String message) {
        super(message);
        this.reason = reason;
        this.reference = reference;
    }

    public Reason reason() {
        return reason;
    }

    public String reference() {
        return reference;
    }

    public enum Reason {
        INVALID_REQUEST,
        RUN_SNAPSHOT_COLLISION,
        SOURCE_NOT_ALLOWED,
        ARTIFACT_MISSING,
        TENANT_MISMATCH,
        VERSION_CHANGED,
        METADATA_MISMATCH,
        INTEGRITY_MISMATCH,
        STALE_EVIDENCE,
        UNSUPPORTED_CONTENT,
        ARTIFACT_TOO_LARGE,
        CONTEXT_BUDGET_EXCEEDED
    }
}
