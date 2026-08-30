package dev.agenticintegrationpatterns.orderdesk.peer;

import java.time.Instant;
import java.util.Set;

/** Untrusted discovery data. It never supplies a credential or executable endpoint. */
public record PeerAdvertisement(
        String peerRef,
        Set<String> capabilities,
        String protocolFamily,
        String protocolVersion,
        String interactionProfile,
        String cardRef,
        String endpointHint,
        Instant expiresAt) {
}
