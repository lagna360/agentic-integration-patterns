package dev.agenticintegrationpatterns.orderdesk.history;

import java.time.Instant;
import java.util.Set;

/** Protected current actor/scope supplied by the authenticated replay entry point, never JSON. */
public record AuthorizedReplayScope(
        String actorRef,
        Set<String> permittedTenantIds,
        Set<String> permittedPurposes,
        String audience,
        String serviceRef,
        Instant expiresAt) {

    public AuthorizedReplayScope {
        if (actorRef == null || actorRef.isBlank()) throw new IllegalArgumentException("actorRef");
        permittedTenantIds = Set.copyOf(permittedTenantIds);
        permittedPurposes = Set.copyOf(permittedPurposes);
        if (audience == null || audience.isBlank()) throw new IllegalArgumentException("audience");
        if (serviceRef == null || serviceRef.isBlank()) throw new IllegalArgumentException("serviceRef");
        if (expiresAt == null) throw new IllegalArgumentException("expiresAt");
    }
}
