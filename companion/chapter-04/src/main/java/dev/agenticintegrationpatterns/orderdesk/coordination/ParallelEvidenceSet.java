package dev.agenticintegrationpatterns.orderdesk.coordination;

import java.time.Instant;
import java.util.List;
import java.util.Set;

public record ParallelEvidenceSet(
        String scatterId,
        String runId,
        String tenantId,
        String planVersion,
        Completion completion,
        Disposition disposition,
        Reason reason,
        CompletionTrigger completionTrigger,
        List<InvestigationReply> replies,
        Set<InvestigationBranch> missingBranches,
        Set<InvestigationBranch> unavailableBranches,
        List<EvidenceConflict> conflicts,
        int duplicateCount,
        Instant closedAt) {

    public ParallelEvidenceSet {
        replies = List.copyOf(replies);
        missingBranches = Set.copyOf(missingBranches);
        unavailableBranches = Set.copyOf(unavailableBranches);
        conflicts = List.copyOf(conflicts);
    }

    public enum Completion {
        COMPLETE,
        PARTIAL,
        CONFLICTED
    }

    public enum Disposition {
        EVIDENCE_READY,
        MANUAL_REVIEW
    }

    public enum Reason {
        ALL_EXPECTED_EVIDENCE,
        OPTIONAL_EVIDENCE_MISSING,
        REQUIRED_EVIDENCE_MISSING,
        EVIDENCE_CONFLICT
    }

    public enum CompletionTrigger {
        ALL_REPLIES,
        TIMEOUT
    }
}
