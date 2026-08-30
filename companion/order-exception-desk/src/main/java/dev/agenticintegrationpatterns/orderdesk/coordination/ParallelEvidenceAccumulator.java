package dev.agenticintegrationpatterns.orderdesk.coordination;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record ParallelEvidenceAccumulator(
        ParallelInvestigationPlan plan,
        Map<InvestigationBranch, InvestigationReply> replies,
        List<EvidenceConflict> conflicts,
        int duplicateCount,
        Instant closedAt,
        ParallelEvidenceSet.CompletionTrigger completionTrigger) {

    public ParallelEvidenceAccumulator {
        replies = Map.copyOf(replies);
        conflicts = List.copyOf(conflicts);
    }

    public boolean closed() {
        return closedAt != null;
    }
}
