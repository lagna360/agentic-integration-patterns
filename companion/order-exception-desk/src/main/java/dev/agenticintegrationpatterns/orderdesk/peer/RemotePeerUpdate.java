package dev.agenticintegrationpatterns.orderdesk.peer;

import java.time.Instant;
import java.util.Set;

public record RemotePeerUpdate(
        String messageId,
        String tenantId,
        String remoteWorkId,
        String peerTaskId,
        Kind kind,
        String protocolFamily,
        String protocolVersion,
        String interactionProfile,
        long cumulativeReportedTokens,
        long cumulativeReportedCostMicros,
        String artifactId,
        String artifactRef,
        String artifactSha256,
        Integer artifactSizeBytes,
        String resultSchemaRef,
        String provenanceRef,
        Instant evidenceObservedAt,
        Instant evidenceValidUntil,
        Set<String> missingDeliverables,
        String reportedExternalEffectRef) {

    public enum Kind {
        ACCEPTED, PARTIAL, COMPLETED, REJECTED, FAILED, CANCELLED,
        INPUT_REQUIRED, AUTH_REQUIRED
    }
}
