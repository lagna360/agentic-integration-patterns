package dev.agenticintegrationpatterns.orderdesk.history;

import java.util.Set;

public record HistoryReceipt(
        String tenantId,
        String observationId,
        Disposition disposition,
        Set<QualitySignal> qualitySignals) {

    public HistoryReceipt {
        qualitySignals = Set.copyOf(qualitySignals);
    }

    public enum Disposition { RECORDED, DUPLICATE, COLLISION }

    public enum QualitySignal {
        TRACE_CONTEXT_ABSENT,
        SEQUENCE_GAP,
        OUT_OF_ORDER,
        FUTURE_SOURCE_TIME,
        DUPLICATE,
        IDENTITY_COLLISION,
        DECLARED_USAGE_ONLY
    }
}
