package dev.agenticintegrationpatterns.orderdesk.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/** Capped exponential backoff with equal jitter, using an injectable entropy sample. */
public final class EqualJitterBackoff {
    private final DoubleSupplier sample;

    public EqualJitterBackoff() {
        this(() -> ThreadLocalRandom.current().nextDouble());
    }

    public EqualJitterBackoff(DoubleSupplier sample) {
        this.sample = Objects.requireNonNull(sample, "sample");
    }

    // tag::equal-jitter-backoff[]
    public Duration delay(Duration base, Duration maximum, int attemptsUsed) {
        long cap = exponentialCap(base.toMillis(), maximum.toMillis(), attemptsUsed);
        long half = cap / 2;
        double entropy = sample.getAsDouble();
        if (entropy < 0.0 || entropy >= 1.0) {
            throw new IllegalStateException("jitter sample must be in [0, 1)");
        }
        return Duration.ofMillis(half + (long) Math.floor(entropy * (cap - half)));
    }
    // end::equal-jitter-backoff[]

    private static long exponentialCap(long base, long maximum, int attemptsUsed) {
        long candidate = base;
        int exponent = Math.max(0, attemptsUsed - 1);
        for (int i = 0; i < exponent && candidate < maximum; i++) {
            candidate = candidate > maximum / 2 ? maximum : candidate * 2;
        }
        return Math.min(candidate, maximum);
    }
}
