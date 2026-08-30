package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;

import java.time.Instant;
import java.util.Arrays;

public record SourceArtifact(
        String tenantId,
        String reference,
        String sourceSystem,
        String sourceVersion,
        Instant observedAt,
        Instant validUntil,
        String contentType,
        InvestigateOrderException.EvidenceTrust trust,
        byte[] content) {

    public SourceArtifact {
        content = content == null ? null : Arrays.copyOf(content, content.length);
    }

    @Override
    public byte[] content() {
        return content == null ? null : Arrays.copyOf(content, content.length);
    }
}
