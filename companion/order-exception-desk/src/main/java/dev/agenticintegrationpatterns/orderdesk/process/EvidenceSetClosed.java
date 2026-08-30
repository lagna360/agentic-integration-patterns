package dev.agenticintegrationpatterns.orderdesk.process;

import java.time.Instant;
import java.util.Set;

public record EvidenceSetClosed(
        String messageId,
        String tenantId,
        String runId,
        String evidenceSetRef,
        String evidenceSetSha256,
        Decision decision,
        Reason reason,
        Set<String> succeededWork,
        Set<String> unavailableWork,
        Set<String> missingWork,
        Instant receivedAt) {

    public EvidenceSetClosed {
        StartInvestigationRun.requireText(messageId, "messageId");
        StartInvestigationRun.requireText(tenantId, "tenantId");
        StartInvestigationRun.requireText(runId, "runId");
        StartInvestigationRun.requireText(evidenceSetRef, "evidenceSetRef");
        if (evidenceSetSha256 == null || !evidenceSetSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("evidenceSetSha256 must be lowercase SHA-256");
        }
        if (decision == null || reason == null || receivedAt == null) {
            throw new IllegalArgumentException("decision, reason, and receivedAt are required");
        }
        succeededWork = Set.copyOf(succeededWork);
        unavailableWork = Set.copyOf(unavailableWork);
        missingWork = Set.copyOf(missingWork);
        var overlap = new java.util.HashSet<>(succeededWork);
        overlap.retainAll(unavailableWork);
        overlap.addAll(intersection(succeededWork, missingWork));
        overlap.addAll(intersection(unavailableWork, missingWork));
        if (!overlap.isEmpty()) {
            throw new IllegalArgumentException("work outcomes must be disjoint");
        }
    }

    private static Set<String> intersection(Set<String> left, Set<String> right) {
        var result = new java.util.HashSet<>(left);
        result.retainAll(right);
        return result;
    }

    public enum Decision {
        EVIDENCE_READY,
        REVIEW_REQUIRED
    }

    public enum Reason {
        ALL_EXPECTED_EVIDENCE,
        OPTIONAL_EVIDENCE_MISSING,
        REQUIRED_EVIDENCE_MISSING,
        EVIDENCE_CONFLICT
    }
}
