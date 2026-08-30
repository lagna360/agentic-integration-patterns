package dev.agenticintegrationpatterns.orderdesk.process;

import java.time.Instant;

public record ProcessOutboxEvent(
        String eventId,
        String tenantId,
        String runId,
        String caseId,
        long aggregateVersion,
        String eventType,
        String payload,
        Instant createdAt) {
}
