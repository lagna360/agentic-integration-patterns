package dev.agenticintegrationpatterns.orderdesk.peer;

public record RemoteTaskReceipt(
        String remoteWorkId,
        String peerTaskId,
        String taskState,
        String disposition,
        String reasonCode,
        long version) {
}
