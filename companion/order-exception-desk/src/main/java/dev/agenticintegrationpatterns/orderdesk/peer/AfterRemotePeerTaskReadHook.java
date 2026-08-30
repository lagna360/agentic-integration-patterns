package dev.agenticintegrationpatterns.orderdesk.peer;

@FunctionalInterface
public interface AfterRemotePeerTaskReadHook {
    void afterRead(String operation, String tenantId, String remoteWorkId, long version);
}
