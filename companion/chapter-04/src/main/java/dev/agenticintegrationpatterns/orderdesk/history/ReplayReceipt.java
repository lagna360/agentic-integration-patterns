package dev.agenticintegrationpatterns.orderdesk.history;

import java.util.Set;

public record ReplayReceipt(
        String tenantId,
        String replayId,
        String replayRunId,
        String sourceRunId,
        String mode,
        String state,
        ReplayEvaluator.ResultCode resultCode,
        String resultSha256,
        Set<ReplayEvaluator.GapCode> explicitGaps) {

    public ReplayReceipt {
        explicitGaps = explicitGaps == null ? Set.of() : Set.copyOf(explicitGaps);
    }
}
