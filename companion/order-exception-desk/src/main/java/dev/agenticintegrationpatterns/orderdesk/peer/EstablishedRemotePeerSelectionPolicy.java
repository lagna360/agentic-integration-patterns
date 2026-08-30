package dev.agenticintegrationpatterns.orderdesk.peer;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;

@Component
public class EstablishedRemotePeerSelectionPolicy {
    private final FixtureProtectedPeerRegistry registry;
    private final Clock clock;

    public EstablishedRemotePeerSelectionPolicy(
            FixtureProtectedPeerRegistry registry, Clock clock) {
        this.registry = registry;
        this.clock = clock;
    }

    // tag::negotiate-peer-contract[]
    public NegotiatedPeerContract negotiate(RemoteTaskDefinition task) {
        PeerAdvertisement ad = Objects.requireNonNull(task.advertisement());
        ProtectedPeerRegistration registration = registry.find(ad.peerRef())
                .orElseThrow(() -> new IllegalArgumentException("PEER_NOT_REGISTERED"));
        boolean compatible = registration.active()
                && registration.permittedTenants().contains(task.tenantId())
                && registration.capability().equals(task.requiredCapability())
                && ad.capabilities().contains(task.requiredCapability())
                && registration.protocolFamily().equals(ad.protocolFamily())
                && registration.protocolVersion().equals(ad.protocolVersion())
                && registration.interactionProfile().equals(ad.interactionProfile())
                && ad.expiresAt() != null && clock.instant().isBefore(ad.expiresAt());
        if (!compatible) throw new IllegalArgumentException("NO_COMPATIBLE_PEER_CONTRACT");

        return new NegotiatedPeerContract(
                registration.peerRef(), registration.capability(),
                registration.protocolFamily(), registration.protocolVersion(),
                registration.interactionProfile(), registration.fixedAdapterRef(),
                registration.revision(), advertisementSha256(ad));
    }
    // end::negotiate-peer-contract[]

    static String advertisementSha256(PeerAdvertisement ad) {
        String canonical = String.join("|", ad.peerRef(),
                ad.capabilities().stream().sorted().reduce("", (a, b) -> a + "," + b),
                ad.protocolFamily(), ad.protocolVersion(), ad.interactionProfile(),
                ad.cardRef(), String.valueOf(ad.endpointHint()), String.valueOf(ad.expiresAt()));
        return sha256(canonical);
    }

    static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
