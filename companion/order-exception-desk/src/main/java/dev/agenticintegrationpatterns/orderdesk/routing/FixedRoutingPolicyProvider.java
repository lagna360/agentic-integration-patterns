package dev.agenticintegrationpatterns.orderdesk.routing;

import dev.agenticintegrationpatterns.orderdesk.context.ResolvedInvestigationContext;
import org.springframework.stereotype.Component;

import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision.Target.INVENTORY_FOLLOW_UP;
import static dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision.Target.ORDER_FOLLOW_UP;
import static dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision.Target.READY_FOR_ASSESSMENT;

@Component
public final class FixedRoutingPolicyProvider implements RoutingPolicyProvider {
    @Override
    public RoutingPolicyContext currentPolicy(ResolvedInvestigationContext context) {
        return new RoutingPolicyContext(
                "orderdesk-routing-v1",
                0.75,
                false,
                Set.of(INVENTORY_FOLLOW_UP, ORDER_FOLLOW_UP, READY_FOR_ASSESSMENT));
    }
}
