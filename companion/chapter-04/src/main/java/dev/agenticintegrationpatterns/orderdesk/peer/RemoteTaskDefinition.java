package dev.agenticintegrationpatterns.orderdesk.peer;

import java.time.Instant;
import java.util.Set;

public record RemoteTaskDefinition(
        String tenantId,
        String remoteWorkId,
        String caseId,
        String correlationId,
        String localOwnerRef,
        String objective,
        Set<String> requiredDeliverables,
        String inputArtifactRef,
        String inputArtifactSha256,
        Instant deadlineAt,
        long maxReportedTokens,
        long maxReportedCostMicros,
        int maxArtifactBytes,
        String requiredCapability,
        String resultSchemaRef,
        PeerAdvertisement advertisement) {
}
