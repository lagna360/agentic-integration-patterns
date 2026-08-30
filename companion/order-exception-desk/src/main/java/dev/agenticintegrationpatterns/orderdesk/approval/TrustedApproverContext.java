package dev.agenticintegrationpatterns.orderdesk.approval;

import java.util.Set;

public record TrustedApproverContext(
        String tenantId,
        String actorRef,
        Set<String> roles,
        String authenticatedChannelRef) {

    public TrustedApproverContext {
        ApprovalSubject.require(tenantId, "tenantId", 120);
        ApprovalSubject.require(actorRef, "actorRef", 240);
        ApprovalSubject.require(authenticatedChannelRef, "authenticatedChannelRef", 240);
        roles = roles == null ? Set.of() : Set.copyOf(roles);
    }

    public boolean hasRole(String role) {
        return roles.contains(role);
    }
}
