package dev.agenticintegrationpatterns.orderdesk.context;

public record ContextUnavailable(
        String runId,
        String tenantId,
        ContextResolutionException.Reason reason) {
}
