package dev.agenticintegrationpatterns.orderdesk.failure;

@FunctionalInterface
public interface FailureClassifier {
    FailureDecision classify(ClassificationObservation observation);
}
