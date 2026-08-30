package dev.agenticintegrationpatterns.orderdesk.security;

import org.springframework.stereotype.Component;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/** A small application policy fixture, not an identity provider or policy engine. */
@Component
public class RouteSecurityPolicy {
    public static final String POLICY_REF = "policy://order-desk/route-security@17.1";
    public static final String POLICY_SHA256 =
            "1ca97251f5d595e6e68081e770dbf90813c8d144edfb2154ee5fd43ff702452a";

    private final Map<String, Set<RouteAction>> entitlements = Map.of(
            "workload:effect-gateway-consumer",
            EnumSet.of(RouteAction.DISPATCH_COMPENSATION),
            "workload:artifact-resolver",
            EnumSet.of(RouteAction.READ_EVIDENCE),
            "workload:capability-gateway",
            EnumSet.of(RouteAction.REQUEST_TOOL),
            "workload:security-operator",
            EnumSet.of(RouteAction.READ_AUDIT));
    public boolean permits(
            String workloadRef, String serviceRef, String tenantId,
            RouteAction action, String targetAccountRef) {
        return "service:order-desk-effect-gateway".equals(serviceRef)
                && tenantId != null
                && (targetAccountRef == null
                    || targetAccountRef.equals("warehouse-account:" + tenantId))
                && entitlements.getOrDefault(workloadRef, Set.of()).contains(action);
    }
}
