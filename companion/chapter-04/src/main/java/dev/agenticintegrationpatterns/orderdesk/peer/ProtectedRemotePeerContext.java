package dev.agenticintegrationpatterns.orderdesk.peer;

import java.time.Instant;
import java.util.Set;

public record ProtectedRemotePeerContext(
        String authenticatedPeerRef,
        Set<String> permittedTenantIds,
        String audience,
        String serviceRef,
        Instant expiresAt) {
}
