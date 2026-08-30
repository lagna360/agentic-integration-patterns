package dev.agenticintegrationpatterns.orderdesk.retry;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** One admitted physical attempt. Completing it does not start another retry. */
public final class RetryAttemptPermit {
    private final ClaimedRetry claim;
    private final JdbcRetrySchedule schedules;
    private final CircuitBreaker circuitBreaker;
    private final RetryCapacityGate capacity;
    private final AtomicBoolean completed = new AtomicBoolean();

    RetryAttemptPermit(
            ClaimedRetry claim,
            JdbcRetrySchedule schedules,
            CircuitBreaker circuitBreaker,
            RetryCapacityGate capacity) {
        this.claim = claim;
        this.schedules = schedules;
        this.circuitBreaker = circuitBreaker;
        this.capacity = capacity;
    }

    public ClaimedRetry claim() {
        return claim;
    }

    public void completeSuccess(Duration duration) {
        finish(duration, null);
    }

    public void completeDependencyFailure(
            Duration duration, RetryableDependencyException failure) {
        finish(duration, Objects.requireNonNull(failure, "failure"));
    }

    /** Consume the permit, leave breaker metrics unchanged, then propagate the defect. */
    public <T extends RuntimeException> void propagateUnexpected(T defect) {
        Objects.requireNonNull(defect, "defect");
        if (!completed.compareAndSet(false, true)) {
            throw new IllegalStateException("retry permit already completed");
        }
        try {
            circuitBreaker.releasePermission();
            schedules.consume(claim);
        } finally {
            capacity.release();
        }
        throw defect;
    }

    private void finish(Duration duration, Throwable failure) {
        Objects.requireNonNull(duration, "duration");
        if (!completed.compareAndSet(false, true)) {
            throw new IllegalStateException("retry permit already completed");
        }
        try {
            if (failure == null) {
                circuitBreaker.onSuccess(duration.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
            } else {
                circuitBreaker.onError(duration.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS,
                        failure);
            }
            schedules.consume(claim);
        } finally {
            capacity.release();
        }
    }
}
