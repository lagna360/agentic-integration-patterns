package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation;

public record ContextResolutionRequest(
        String runId,
        AdmittedInvestigation admitted) {
}
