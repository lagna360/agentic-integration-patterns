package dev.agenticintegrationpatterns.orderdesk.context;

import java.time.Instant;
import java.util.List;

public record ContextSnapshot(
        int schemaVersion,
        String snapshotId,
        String tenantId,
        String runId,
        String commandId,
        String admittedWorkFingerprint,
        String instructionSetRef,
        String policySetRef,
        String capabilityCatalogRef,
        Instant createdAt,
        String selectionPolicyVersion,
        String tokenEstimatorVersion,
        int maxContextBytes,
        int maxEstimatedTokens,
        int usedContextBytes,
        int estimatedTokens,
        List<ContextArtifact> artifacts) {

    public ContextSnapshot {
        artifacts = List.copyOf(artifacts);
    }
}
