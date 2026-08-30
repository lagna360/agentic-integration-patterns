package dev.agenticintegrationpatterns.orderdesk.security;

/** Untrusted message data. It deliberately contains no authenticated context or grant object. */
public record SecuredRouteMessage(
        String messageId,
        String producerProvenanceRef,
        String producerClaimRef,
        String claimedTenantId,
        String subjectRef,
        String delegationAuthorityRef,
        RouteAction action,
        String resourceRef,
        String claimedAuthorityRef,
        long expectedPlanVersion,
        long expectedEffectVersion,
        String breakGlassGrantRef) {
}
