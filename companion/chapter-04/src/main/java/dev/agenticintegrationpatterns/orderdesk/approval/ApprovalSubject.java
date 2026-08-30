package dev.agenticintegrationpatterns.orderdesk.approval;

import java.time.Instant;
import java.util.List;

/** Exact immutable subject evaluated by policy and, when required, a human. */
// tag::approval-subject[]
public record ApprovalSubject(
        String tenantId,
        String runId,
        String caseId,
        String proposalEventId,
        String proposalId,
        String category,
        String effectId,
        String warehouseId,
        String sku,
        int quantity,
        String effectIntentSha256,
        String evidenceSetRef,
        String evidenceSetSha256,
        String contextSnapshotId,
        List<String> configurationRefs,
        long incrementalShippingCostMinor,
        String currency,
        Instant evidenceValidUntil,
        String proposerRef,
        String subjectDigestVersion,
        RiskClass riskClass) {

    public ApprovalSubject {
        require(tenantId, "tenantId", 120);
        require(runId, "runId", 160);
        require(caseId, "caseId", 160);
        require(proposalEventId, "proposalEventId", 160);
        require(proposalId, "proposalId", 160);
        require(category, "category", 80);
        require(effectId, "effectId", 160);
        require(warehouseId, "warehouseId", 160);
        require(sku, "sku", 160);
        requireSha(effectIntentSha256, "effectIntentSha256");
        require(evidenceSetRef, "evidenceSetRef", 600);
        requireSha(evidenceSetSha256, "evidenceSetSha256");
        require(contextSnapshotId, "contextSnapshotId", 160);
        configurationRefs = configurationRefs == null ? List.of() : List.copyOf(configurationRefs);
        if (configurationRefs.isEmpty() || configurationRefs.size() > 8) {
            throw new IllegalArgumentException("one to eight configuration references are required");
        }
        configurationRefs.forEach(value -> require(value, "configurationRef", 240));
        require(currency, "currency", 3);
        require(proposerRef, "proposerRef", 240);
        require(subjectDigestVersion, "subjectDigestVersion", 80);
        if (quantity < 1 || quantity > 10_000) {
            throw new IllegalArgumentException("quantity must be between 1 and 10000");
        }
        if (evidenceValidUntil == null || riskClass == null) {
            throw new IllegalArgumentException("evidence validity and risk class are required");
        }
    }

    public enum RiskClass {
        LOW,
        ALTERNATE_WAREHOUSE_SPLIT,
        HIGH,
        FORBIDDEN,
        UNKNOWN
    }

    static void require(String value, String name, int max) {
        if (value == null || value.isBlank() || value.length() > max) {
            throw new IllegalArgumentException(name + " is missing or too long");
        }
    }

    private static void requireSha(String value, String name) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(name + " must be lowercase SHA-256");
        }
    }
}
// end::approval-subject[]
