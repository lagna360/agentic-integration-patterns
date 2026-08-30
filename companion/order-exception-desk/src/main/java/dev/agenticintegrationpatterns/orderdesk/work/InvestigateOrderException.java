package dev.agenticintegrationpatterns.orderdesk.work;

import java.time.Instant;
import java.util.List;

// tag::contract[]
public record InvestigateOrderException(
        int schemaVersion,
        String commandId,
        String type,
        String caseId,
        String correlationId,
        String causedBy,
        Instant issuedAt,
        Instant deadlineAt,
        String tenantId,
        String principalRef,
        String requestingWorkloadRef,
        WorkObjective objective,
        ExecutionLimits limits,
        List<String> requestedCapabilities,
        List<EvidenceReference> evidence,
        ConfigurationReferences configuration,
        String replyContract) {

    public InvestigateOrderException {
        requestedCapabilities = requestedCapabilities == null
                ? null : List.copyOf(requestedCapabilities);
        evidence = evidence == null ? null : List.copyOf(evidence);
    }

    public record WorkObjective(String code, String version) {}

    public record ExecutionLimits(
            int maxModelTurns,
            int maxToolRequests,
            int maxOutputTokens) {}

    public record EvidenceReference(
            String reference,
            String sourceSystem,
            String sourceVersion,
            Instant observedAt,
            Instant validUntil,
            EvidenceTrust trust) {}

    public enum EvidenceTrust {
        AUTHORITATIVE_SOURCE,
        UNTRUSTED_TEXT
    }

    public record ConfigurationReferences(
            String instructionSetRef,
            String policySetRef,
            String capabilityCatalogRef) {}
}
// end::contract[]
