package dev.agenticintegrationpatterns.orderdesk.approval;

import java.time.Instant;

public record ApprovalReceipt(
        Disposition disposition,
        String tenantId,
        String requestId,
        State state,
        long version,
        String subjectSha256,
        Instant authorityValidUntil,
        String authorityRef) {

    public enum Disposition {
        CREATED,
        APPLIED,
        DUPLICATE_SAME,
        IDENTITY_COLLISION,
        STALE_VERSION,
        NOT_DUE,
        DENIED,
        LATE,
        EXPIRED
    }

    public enum State {
        PENDING,
        ESCALATED,
        APPROVED,
        AUTO_AUTHORIZED,
        FORBIDDEN,
        INDETERMINATE,
        REJECTED,
        CHANGES_REQUESTED,
        EXPIRED,
        REVOKED,
        SUPERSEDED
    }
}
