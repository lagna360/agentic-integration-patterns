package dev.agenticintegrationpatterns.orderdesk.model;

import java.time.Instant;

public record AuditRecord(
        Instant recordedAt,
        String correlationId,
        String tenantId,
        String caseId,
        String outcomeType,
        String provider,
        String model,
        String instructionVersion) {
}
