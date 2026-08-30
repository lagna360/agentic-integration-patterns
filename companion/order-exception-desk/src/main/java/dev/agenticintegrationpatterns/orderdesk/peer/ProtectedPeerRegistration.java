package dev.agenticintegrationpatterns.orderdesk.peer;

import java.util.Set;

/** Server-owned selection policy; the fixed adapter reference never comes from discovery. */
public record ProtectedPeerRegistration(
        String peerRef,
        Set<String> permittedTenants,
        String capability,
        String protocolFamily,
        String protocolVersion,
        String interactionProfile,
        String fixedAdapterRef,
        long revision,
        boolean active) {
}
