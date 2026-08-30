package dev.agenticintegrationpatterns.orderdesk.coordination;

public record EvidenceConflict(
        InvestigationBranch firstBranch,
        InvestigationBranch secondBranch,
        String evidenceKey,
        String firstValueSha256,
        String secondValueSha256) {
}
