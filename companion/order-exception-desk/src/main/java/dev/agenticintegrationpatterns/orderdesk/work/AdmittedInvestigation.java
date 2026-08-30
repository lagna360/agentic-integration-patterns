package dev.agenticintegrationpatterns.orderdesk.work;

import java.util.Set;

public record AdmittedInvestigation(
        InvestigateOrderException command,
        TrustedAdmissionContext trustedContext,
        Set<String> effectiveCapabilities,
        InvestigateOrderException.ExecutionLimits effectiveLimits,
        ReplyContract replyContract) {

    public AdmittedInvestigation {
        effectiveCapabilities = Set.copyOf(effectiveCapabilities);
    }

    public enum ReplyContract {
        ORDER_EXCEPTION_CASE_RESULTS_V1
    }
}
