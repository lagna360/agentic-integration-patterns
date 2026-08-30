package dev.agenticintegrationpatterns.orderdesk.approval;

import java.time.Duration;

public record ApprovalPolicyDecision(
        String decisionId,
        String policyRef,
        Disposition disposition,
        String requiredRole,
        int requiredApprovals,
        Duration decisionWindow,
        Duration authorityWindow,
        String reasonCode) {

    public enum Disposition {
        FORBIDDEN,
        AUTO_AUTHORIZED,
        HUMAN_REQUIRED,
        INDETERMINATE
    }
}
