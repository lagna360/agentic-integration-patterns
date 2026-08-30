package dev.agenticintegrationpatterns.orderdesk.coordination;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public final class InvestigationRecipientRegistry {
    public List<String> endpoints(ParallelInvestigationPlan plan) {
        return plan.expectedBranches().stream()
                .sorted()
                .map(this::endpoint)
                .toList();
    }

    private String endpoint(InvestigationBranch branch) {
        return switch (branch) {
            case INVENTORY_RECHECK -> "direct:parallel-inventory-recheck";
            case ORDER_HISTORY -> "direct:parallel-order-history";
        };
    }
}
