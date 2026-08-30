package dev.agenticintegrationpatterns.orderdesk.capability;

import dev.agenticintegrationpatterns.orderdesk.context.ResolvedInvestigationContext;

public record CapabilityInvocationRequest(
        ResolvedInvestigationContext context,
        ToolCallIntent intent,
        int completedToolRequests) {
}
