package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;

import java.time.Instant;

public record ContextArtifact(
        String artifactId,
        String viewId,
        String reference,
        String sourceSystem,
        String sourceVersion,
        Instant observedAt,
        Instant retrievedAt,
        Instant validUntil,
        String contentType,
        InvestigateOrderException.EvidenceTrust trust,
        int sourceSizeBytes,
        String sourceSha256,
        int viewSizeBytes,
        String viewSha256,
        String normalizationVersion,
        String redactionPolicyVersion,
        String modelSafeText) {
}
