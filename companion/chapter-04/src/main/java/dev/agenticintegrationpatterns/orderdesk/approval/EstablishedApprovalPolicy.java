package dev.agenticintegrationpatterns.orderdesk.approval;

import org.springframework.stereotype.Component;

import java.time.Duration;

import static dev.agenticintegrationpatterns.orderdesk.approval.ApprovalPolicyDecision.Disposition.*;

@Component
public class EstablishedApprovalPolicy implements ApprovalPolicy {
    private static final String POLICY = "policy://tenant-ca/order-effects/v3";
    private static final long AUTO_THRESHOLD_MINOR = 2_500;
    private static final long TWO_PERSON_THRESHOLD_MINOR = 10_000;

    // tag::approval-policy[]
    @Override
    public ApprovalPolicyDecision evaluate(ApprovalSubject subject) {
        if (subject.riskClass() == ApprovalSubject.RiskClass.FORBIDDEN) {
            return decision(subject, FORBIDDEN, 0, "EFFECT_FORBIDDEN_BY_POLICY");
        }
        if (subject.riskClass() == ApprovalSubject.RiskClass.UNKNOWN
                || subject.incrementalShippingCostMinor() < 0
                || !"CAD".equals(subject.currency())) {
            return decision(subject, INDETERMINATE, 0, "POLICY_FACTS_INCOMPLETE");
        }
        if (subject.incrementalShippingCostMinor() <= AUTO_THRESHOLD_MINOR) {
            return decision(subject, AUTO_AUTHORIZED, 0, "BELOW_INCREMENTAL_COST_THRESHOLD");
        }
        if (subject.incrementalShippingCostMinor() > TWO_PERSON_THRESHOLD_MINOR) {
            return decision(subject, HUMAN_REQUIRED, 2, "HIGH_COST_TWO_PERSON_APPROVAL");
        }
        return decision(subject, HUMAN_REQUIRED, 1, "INCREMENTAL_COST_REQUIRES_APPROVAL");
    }
    // end::approval-policy[]

    private static ApprovalPolicyDecision decision(
            ApprovalSubject subject,
            ApprovalPolicyDecision.Disposition disposition,
            int approvals,
            String reason) {
        return new ApprovalPolicyDecision(
                "policy-decision:" + subject.proposalId(), POLICY, disposition,
                "ORDER_EXCEPTION_APPROVER", approvals,
                Duration.ofMinutes(5), Duration.ofMinutes(10), reason);
    }
}
