package dev.agenticintegrationpatterns.orderdesk.retry;

public interface RetryCapacityGate {
    boolean tryAcquire();
    void release();
}
