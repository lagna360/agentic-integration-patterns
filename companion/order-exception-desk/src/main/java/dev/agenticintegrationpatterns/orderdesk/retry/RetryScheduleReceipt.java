package dev.agenticintegrationpatterns.orderdesk.retry;

import java.time.Instant;

public record RetryScheduleReceipt(
        Disposition disposition,
        String tenantId,
        String scheduleId,
        State state,
        long version,
        int attemptsUsed,
        long tokensUsed,
        long costUsedMicros,
        Instant notBefore) {

    public enum Disposition {
        CREATED,
        DUPLICATE_SAME,
        IDENTITY_COLLISION
    }

    public enum State {
        SCHEDULED,
        CLAIMED,
        CONSUMED
    }
}
