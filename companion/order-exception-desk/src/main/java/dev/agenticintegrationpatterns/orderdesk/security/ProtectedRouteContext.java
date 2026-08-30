package dev.agenticintegrationpatterns.orderdesk.security;

import java.time.Instant;
import java.util.Set;

/**
 * Context constructed by a trusted transport adapter. It contains identity references,
 * never bearer tokens, passwords, private keys, or provider credentials.
 */
public record ProtectedRouteContext(
        String authenticatedWorkloadRef,
        String serviceRef,
        String actorRef,
        Set<String> permittedTenantIds,
        String audience,
        String authenticationMethod,
        String credentialKeyId,
        Set<String> roles,
        String assuranceLevel,
        Instant authenticatedAt,
        Instant expiresAt) {

    public ProtectedRouteContext {
        permittedTenantIds = permittedTenantIds == null ? Set.of() : Set.copyOf(permittedTenantIds);
        roles = roles == null ? Set.of() : Set.copyOf(roles);
    }
}
