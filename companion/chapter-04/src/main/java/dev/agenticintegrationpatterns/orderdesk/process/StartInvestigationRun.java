package dev.agenticintegrationpatterns.orderdesk.process;

import java.time.Instant;
import java.util.Set;

public record StartInvestigationRun(
        String messageId,
        String tenantId,
        String runId,
        String caseId,
        String correlationId,
        String commandId,
        String planVersion,
        Instant deadlineAt,
        Set<String> expectedWork,
        Set<String> requiredWork,
        Instant receivedAt) {

    public StartInvestigationRun {
        requireText(messageId, "messageId");
        requireText(tenantId, "tenantId");
        requireText(runId, "runId");
        requireText(caseId, "caseId");
        requireText(correlationId, "correlationId");
        requireText(commandId, "commandId");
        requireText(planVersion, "planVersion");
        if (deadlineAt == null || receivedAt == null || !deadlineAt.isAfter(receivedAt)) {
            throw new IllegalArgumentException("deadlineAt must be after receivedAt");
        }
        expectedWork = Set.copyOf(expectedWork);
        requiredWork = Set.copyOf(requiredWork);
        if (expectedWork.isEmpty() || !expectedWork.containsAll(requiredWork)) {
            throw new IllegalArgumentException("requiredWork must be a subset of non-empty expectedWork");
        }
    }

    static void requireText(String value, String name) {
        if (value == null || value.isBlank() || value.length() > 600) {
            throw new IllegalArgumentException(name + " is missing or too long");
        }
    }
}
