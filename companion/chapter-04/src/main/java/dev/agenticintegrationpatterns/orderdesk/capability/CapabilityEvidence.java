package dev.agenticintegrationpatterns.orderdesk.capability;

import java.time.Instant;

public record CapabilityEvidence(
        String evidenceId,
        String runId,
        String tenantId,
        String callId,
        String capabilityName,
        String catalogVersion,
        String argumentSchemaId,
        String argumentsSha256,
        String resultSha256,
        Instant executedAt,
        InventoryAvailabilityObservation observation) {
}
