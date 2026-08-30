package dev.agenticintegrationpatterns.orderdesk.retry;

/** A classified dependency failure that is eligible for breaker accounting. */
public final class RetryableDependencyException extends RuntimeException {
    public RetryableDependencyException(String message) {
        super(message);
    }
}
