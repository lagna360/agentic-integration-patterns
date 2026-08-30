package dev.agenticintegrationpatterns.orderdesk.coordination;

import java.time.Instant;

public record InvestigationReply(
        String replyId,
        String scatterId,
        String runId,
        String tenantId,
        InvestigationBranch branch,
        Status status,
        EvidenceFinding finding,
        Instant completedAt) {

    public enum Status {
        SUCCEEDED,
        UNAVAILABLE
    }
}
