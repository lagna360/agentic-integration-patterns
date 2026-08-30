package dev.agenticintegrationpatterns.orderdesk.recovery;

@FunctionalInterface
public interface AfterResolutionStateHook {
    void afterStateMutation(String tenantId, String planId, ResolutionReceipt.State state, long version);
}
