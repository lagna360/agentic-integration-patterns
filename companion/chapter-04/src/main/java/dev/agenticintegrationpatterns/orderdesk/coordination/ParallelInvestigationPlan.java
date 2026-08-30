package dev.agenticintegrationpatterns.orderdesk.coordination;

import java.time.Instant;
import java.util.Set;

public record ParallelInvestigationPlan(
        String scatterId,
        String planVersion,
        String runId,
        String tenantId,
        Instant deadlineAt,
        Set<InvestigationBranch> expectedBranches,
        Set<InvestigationBranch> requiredBranches,
        boolean allowPartialWhenRequiredComplete) {

    public ParallelInvestigationPlan {
        expectedBranches = Set.copyOf(expectedBranches);
        requiredBranches = Set.copyOf(requiredBranches);
    }
}
