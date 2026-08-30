package dev.agenticintegrationpatterns.orderdesk.routing;

import java.util.Set;

public record RoutingPolicyContext(
        String policyVersion,
        double minimumSupportScore,
        boolean forceManualReview,
        Set<RoutingDecision.Target> allowedTargets) {

    public RoutingPolicyContext {
        allowedTargets = allowedTargets == null ? null : Set.copyOf(allowedTargets);
    }
}
