package dev.agenticintegrationpatterns.orderdesk.retry;

import org.springframework.stereotype.Component;

@Component
public final class RetryControlService {
    private final EstablishedRetryPolicy policy;
    private final JdbcRetrySchedule schedules;

    public RetryControlService(EstablishedRetryPolicy policy, JdbcRetrySchedule schedules) {
        this.policy = policy;
        this.schedules = schedules;
    }

    public RetrySchedulingResult schedule(RetryPolicyRequest request) {
        var decision = policy.decide(request);
        var receipt = decision.disposition() == RetryPolicyDecision.Disposition.SCHEDULED
                ? schedules.schedule(request, decision) : null;
        return new RetrySchedulingResult(decision, receipt);
    }
}
