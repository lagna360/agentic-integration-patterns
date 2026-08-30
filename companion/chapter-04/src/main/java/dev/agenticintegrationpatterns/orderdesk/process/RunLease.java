package dev.agenticintegrationpatterns.orderdesk.process;

import java.time.Instant;

public record RunLease(
        String tenantId,
        String runId,
        String owner,
        Purpose purpose,
        long fenceToken,
        long version,
        Instant leaseUntil,
        Instant deadlineAt) {

    public enum Purpose {
        WORK,
        DEADLINE
    }
}
