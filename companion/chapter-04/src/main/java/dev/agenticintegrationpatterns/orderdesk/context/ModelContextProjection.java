package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;

import java.util.List;

// tag::model-context-projection[]
public record ModelContextProjection(
        String snapshotId,
        String instructionSetRef,
        List<EvidenceBlock> evidence) {

    public ModelContextProjection {
        evidence = List.copyOf(evidence);
    }

    public record EvidenceBlock(
            String artifactId,
            String sourceReference,
            InvestigateOrderException.EvidenceTrust trust,
            String content) {
    }
}
// end::model-context-projection[]
