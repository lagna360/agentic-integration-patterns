package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;

public interface ArtifactSource {
    boolean supports(String sourceSystem);

    SourceArtifact acquire(
            String tenantId,
            InvestigateOrderException.EvidenceReference reference);
}
