package dev.agenticintegrationpatterns.orderdesk.retry;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.springframework.stereotype.Component;

import static dev.agenticintegrationpatterns.orderdesk.retry.RetryAdmissionResult.Disposition.*;

@Component
public final class RetryAttemptAdmission {
    private final JdbcRetrySchedule schedules;
    private final CircuitBreaker circuitBreaker;
    private final RetryCapacityGate capacity;

    public RetryAttemptAdmission(
            JdbcRetrySchedule schedules,
            CircuitBreaker circuitBreaker,
            RetryCapacityGate capacity) {
        this.schedules = schedules;
        this.circuitBreaker = circuitBreaker;
        this.capacity = capacity;
    }

    // tag::retry-attempt-admission[]
    public RetryAdmissionResult admit(RetryClaimCommand command) {
        if (!capacity.tryAcquire()) {
            return RetryAdmissionResult.refused(CAPACITY_FULL);
        }
        if (!circuitBreaker.tryAcquirePermission()) {
            capacity.release();
            return RetryAdmissionResult.refused(CIRCUIT_OPEN);
        }
        final java.util.Optional<ClaimedRetry> claim;
        try {
            claim = schedules.claimDue(command.tenantId(), command.scheduleId(),
                    command.workerId(), command.leaseDuration());
        } catch (RuntimeException unexpected) {
            circuitBreaker.releasePermission();
            capacity.release();
            throw unexpected;
        }
        if (claim.isEmpty()) {
            circuitBreaker.releasePermission();
            capacity.release();
            return RetryAdmissionResult.refused(NOT_DUE_OR_ALREADY_CLAIMED);
        }
        return new RetryAdmissionResult(PERMITTED,
                new RetryAttemptPermit(claim.get(), schedules, circuitBreaker, capacity));
    }
    // end::retry-attempt-admission[]
}
