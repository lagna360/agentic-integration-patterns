package dev.agenticintegrationpatterns.orderdesk.security;

import java.time.Instant;
import java.util.Set;

public record BreakGlassGrant(
        String grantRef,
        String issuerRef,
        String policyRef,
        String audience,
        String serviceRef,
        String actorRef,
        String tenantId,
        Set<RouteAction> actions,
        Set<String> resourceRefs,
        String reasonCode,
        Instant issuedAt,
        Instant expiresAt,
        boolean revoked,
        boolean superseded,
        String requiredAssurance,
        String requiredRole,
        String secondActorRef) {

    public BreakGlassGrant {
        actions = actions == null ? Set.of() : Set.copyOf(actions);
        resourceRefs = resourceRefs == null ? Set.of() : Set.copyOf(resourceRefs);
    }
}
