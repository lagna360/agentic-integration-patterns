package dev.agenticintegrationpatterns.orderdesk.retry;

import java.util.concurrent.Semaphore;

public final class SemaphoreRetryCapacityGate implements RetryCapacityGate {
    private final Semaphore capacity;

    public SemaphoreRetryCapacityGate(int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be positive");
        }
        capacity = new Semaphore(permits);
    }

    @Override
    public boolean tryAcquire() {
        return capacity.tryAcquire();
    }

    @Override
    public void release() {
        capacity.release();
    }
}
