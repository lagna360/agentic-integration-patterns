package dev.agenticintegrationpatterns.orderdesk.retry;

import java.time.Duration;

public record RetryClaimCommand(
        String tenantId,
        String scheduleId,
        String workerId,
        Duration leaseDuration) {
}
