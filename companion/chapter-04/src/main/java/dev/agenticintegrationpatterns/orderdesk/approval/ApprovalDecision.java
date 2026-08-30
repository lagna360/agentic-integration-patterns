package dev.agenticintegrationpatterns.orderdesk.approval;

public record ApprovalDecision(
        String decisionId,
        String requestId,
        long expectedVersion,
        Action action,
        String reasonCode) {

    public ApprovalDecision {
        ApprovalSubject.require(decisionId, "decisionId", 160);
        ApprovalSubject.require(requestId, "requestId", 160);
        ApprovalSubject.require(reasonCode, "reasonCode", 120);
        if (expectedVersion < 0 || action == null) {
            throw new IllegalArgumentException("expected version and action are required");
        }
    }

    public enum Action {
        APPROVE,
        REJECT,
        REQUEST_CHANGE,
        ESCALATE,
        REVOKE,
        SUPERSEDE
    }
}
