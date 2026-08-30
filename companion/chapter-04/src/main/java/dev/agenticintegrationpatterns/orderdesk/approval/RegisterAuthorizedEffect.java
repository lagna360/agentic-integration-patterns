package dev.agenticintegrationpatterns.orderdesk.approval;

import dev.agenticintegrationpatterns.orderdesk.effect.ReserveInventoryEffect;

public record RegisterAuthorizedEffect(
        String requestId,
        ApprovalSubject subject,
        ReserveInventoryEffect effect) {

    public RegisterAuthorizedEffect {
        ApprovalSubject.require(requestId, "requestId", 160);
        if (subject == null || effect == null) {
            throw new IllegalArgumentException("subject and effect are required");
        }
    }
}
