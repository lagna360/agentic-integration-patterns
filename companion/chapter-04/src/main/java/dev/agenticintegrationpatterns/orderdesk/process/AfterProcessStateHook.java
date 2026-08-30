package dev.agenticintegrationpatterns.orderdesk.process;

@FunctionalInterface
public interface AfterProcessStateHook {
    void afterStateMutation(String tenantId, String runId, long newVersion);
}
