package dev.agenticintegrationpatterns.orderdesk.coordination;

import dev.agenticintegrationpatterns.orderdesk.context.ResolvedInvestigationContext;

public record ParallelInvestigationRequest(ResolvedInvestigationContext context) {
}
