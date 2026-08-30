package dev.agenticintegrationpatterns.orderdesk.routing;

import dev.agenticintegrationpatterns.orderdesk.context.ResolvedInvestigationContext;

@FunctionalInterface
public interface RoutingPolicyProvider {
    RoutingPolicyContext currentPolicy(ResolvedInvestigationContext context);
}
