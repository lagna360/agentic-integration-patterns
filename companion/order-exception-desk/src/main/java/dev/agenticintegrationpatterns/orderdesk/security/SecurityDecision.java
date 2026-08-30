package dev.agenticintegrationpatterns.orderdesk.security;

import java.time.Instant;

public record SecurityDecision(
        String decisionId,
        String messageId,
        String messageSha256,
        String tenantId,
        String authenticatedWorkloadRef,
        String serviceRef,
        String actorRef,
        String actionName,
        String resourceRef,
        String producerProvenanceRef,
        String producerClaimSha256,
        String tenantClaimSha256,
        String resourceClaimSha256,
        String subjectClaimSha256,
        String delegationClaimSha256,
        String audience,
        String policyRef,
        String policySha256,
        Outcome outcome,
        String reasonCode,
        String invalidFieldCode,
        String breakGlassGrantRef,
        Instant decidedAt,
        Instant contextExpiresAt) {

    public enum Outcome { ALLOW, DENY }
}
