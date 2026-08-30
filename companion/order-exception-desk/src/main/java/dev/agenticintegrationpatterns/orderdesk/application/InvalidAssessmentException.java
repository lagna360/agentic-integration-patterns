package dev.agenticintegrationpatterns.orderdesk.application;

public class InvalidAssessmentException extends RuntimeException {
    public InvalidAssessmentException(String message) {
        super(message);
    }
}
