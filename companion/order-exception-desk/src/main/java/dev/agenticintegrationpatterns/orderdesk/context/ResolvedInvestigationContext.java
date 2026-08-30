package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation;

public record ResolvedInvestigationContext(
        AdmittedInvestigation admitted,
        ContextSnapshot snapshot,
        ModelContextProjection modelContext) {
}
