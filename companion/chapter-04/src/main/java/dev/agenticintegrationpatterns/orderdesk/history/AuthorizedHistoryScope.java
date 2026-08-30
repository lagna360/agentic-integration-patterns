package dev.agenticintegrationpatterns.orderdesk.history;

import java.time.Instant;
import java.util.Set;

/** Trusted output of the existing security gate, not credentials supplied by a message. */
public record AuthorizedHistoryScope(
        String actorRef,
        Set<String> tenantIds,
        Set<HistoryView.Purpose> purposes,
        Instant expiresAt) {

    public AuthorizedHistoryScope {
        tenantIds = Set.copyOf(tenantIds);
        purposes = Set.copyOf(purposes);
    }

    void require(String tenantId, HistoryView.Purpose purpose, Instant now) {
        if (!now.isBefore(expiresAt)) throw new SecurityException("HISTORY_SCOPE_EXPIRED");
        if (!tenantIds.contains(tenantId)) throw new SecurityException("HISTORY_TENANT_DENIED");
        if (!purposes.contains(purpose)) throw new SecurityException("HISTORY_PURPOSE_DENIED");
    }
}
