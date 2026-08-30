package dev.agenticintegrationpatterns.orderdesk.context;

public interface TokenEstimator {
    String version();

    int estimate(String text);
}
