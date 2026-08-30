package dev.agenticintegrationpatterns.orderdesk.history;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/** A purpose-bound projection over retained facts, never a replacement source of business state. */
public record HistoryView(
        Purpose purpose,
        String tenantId,
        String caseId,
        List<Fact> facts,
        Set<String> explicitGaps) {

    public enum Purpose { OPERATIONS, AUDIT, EVALUATION, RECOVERY }

    public record Fact(
            String observationId,
            long sourceSequence,
            String eventClass,
            String outcomeCode,
            Instant occurredAt,
            Instant recordedAt,
            boolean traceContextPresent,
            String usageSource,
            Long usageTokens,
            String summaryCode) {}
}
