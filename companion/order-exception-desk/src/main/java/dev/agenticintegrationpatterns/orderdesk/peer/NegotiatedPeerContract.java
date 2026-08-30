package dev.agenticintegrationpatterns.orderdesk.peer;

public record NegotiatedPeerContract(
        String peerRef,
        String capability,
        String protocolFamily,
        String protocolVersion,
        String interactionProfile,
        String fixedAdapterRef,
        long registrationRevision,
        String advertisementSha256) {
}
