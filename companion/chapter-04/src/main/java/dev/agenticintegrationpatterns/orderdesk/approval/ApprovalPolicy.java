package dev.agenticintegrationpatterns.orderdesk.approval;

@FunctionalInterface
public interface ApprovalPolicy {
    ApprovalPolicyDecision evaluate(ApprovalSubject subject);
}
