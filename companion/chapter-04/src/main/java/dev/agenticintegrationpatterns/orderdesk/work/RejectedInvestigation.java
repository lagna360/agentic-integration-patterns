package dev.agenticintegrationpatterns.orderdesk.work;

import java.time.Instant;

public record RejectedInvestigation(
        InvalidWorkEnvelopeException.Violation violation,
        String commandId,
        String correlationId,
        String claimedTenantId,
        Instant receivedAt,
        String detail) {
}
