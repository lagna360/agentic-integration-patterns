package dev.agenticintegrationpatterns.orderdesk.approval;

@FunctionalInterface
public interface AfterApprovalStateHook {
    void afterStateMutation(String tenantId, String requestId, ApprovalReceipt.State state, long version);
}
