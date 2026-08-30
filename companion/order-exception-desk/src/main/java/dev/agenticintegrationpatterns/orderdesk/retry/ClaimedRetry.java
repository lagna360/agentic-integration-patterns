package dev.agenticintegrationpatterns.orderdesk.retry;

import java.time.Instant;

public record ClaimedRetry(
        String tenantId,
        String scheduleId,
        String runId,
        String operationKey,
        String owner,
        long version,
        Instant claimUntil,
        java.time.Duration attemptTimeout,
        int attemptsUsed,
        long tokensUsed,
        long costUsedMicros) {
}
