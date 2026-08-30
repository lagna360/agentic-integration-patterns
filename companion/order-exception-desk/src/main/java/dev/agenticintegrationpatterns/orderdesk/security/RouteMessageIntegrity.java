package dev.agenticintegrationpatterns.orderdesk.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/** Canonical digest for the security-relevant accepted record fields. */
final class RouteMessageIntegrity {
    private RouteMessageIntegrity() {
    }

    static String sha256(SecuredRouteMessage message) {
        if (message == null) return null;
        StringBuilder canonical = new StringBuilder("secured-route-message:v1|");
        append(canonical, message.messageId());
        append(canonical, message.producerProvenanceRef());
        append(canonical, message.producerClaimRef());
        append(canonical, message.claimedTenantId());
        append(canonical, message.subjectRef());
        append(canonical, message.delegationAuthorityRef());
        append(canonical, message.action() == null ? null : message.action().name());
        append(canonical, message.resourceRef());
        append(canonical, message.claimedAuthorityRef());
        append(canonical, Long.toString(message.expectedPlanVersion()));
        append(canonical, Long.toString(message.expectedEffectVersion()));
        append(canonical, message.breakGlassGrantRef());
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void append(StringBuilder target, String value) {
        if (value == null) {
            target.append("-1:|");
        } else {
            target.append(value.length()).append(':').append(value).append('|');
        }
    }
}
