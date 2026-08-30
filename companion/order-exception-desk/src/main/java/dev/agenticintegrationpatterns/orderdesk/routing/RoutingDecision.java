package dev.agenticintegrationpatterns.orderdesk.routing;

public record RoutingDecision(
        String runId,
        String tenantId,
        Target target,
        Reason reason,
        String policyVersion,
        String advisoryClass,
        Double advisorySupportScore) {

    public enum Target {
        INVENTORY_FOLLOW_UP,
        ORDER_FOLLOW_UP,
        READY_FOR_ASSESSMENT,
        MANUAL_REVIEW,
        INVESTIGATION_STOPPED
    }

    public enum Reason {
        ADVISORY_ACCEPTED,
        DEADLINE_EXCEEDED,
        INVALID_POLICY,
        POLICY_OVERRIDE,
        INVALID_ASSESSMENT,
        UNVERIFIED_EVIDENCE,
        BELOW_SUPPORT_THRESHOLD,
        TARGET_NOT_ALLOWED
    }
}
