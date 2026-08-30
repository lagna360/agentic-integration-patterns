package dev.agenticintegrationpatterns.orderdesk.security;

/** Internal value assembled by a trusted transport adapter, never deserialized from the record. */
public record SecurityAdmission(
        SecuredRouteMessage message,
        ProtectedRouteContext protectedContext) {
}
