package dev.agenticintegrationpatterns.orderdesk.history;

import java.util.List;

public record ReplayCommand(
        String tenantId,
        String replayId,
        String authorizationId,
        String manifestId) {

    public ReplayCommand {
        requireText(tenantId, "tenantId");
        requireText(replayId, "replayId");
        requireText(authorizationId, "authorizationId");
        requireText(manifestId, "manifestId");
    }

    public String calculatedSha256() {
        return ReplayInputManifest.canonicalSha256(
                List.of(tenantId, replayId, authorizationId, manifestId));
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field);
    }
}
