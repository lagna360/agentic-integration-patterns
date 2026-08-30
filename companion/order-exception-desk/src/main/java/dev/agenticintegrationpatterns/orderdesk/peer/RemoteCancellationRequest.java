package dev.agenticintegrationpatterns.orderdesk.peer;

public record RemoteCancellationRequest(
        String tenantId, String remoteWorkId, String reasonCode) {
}
