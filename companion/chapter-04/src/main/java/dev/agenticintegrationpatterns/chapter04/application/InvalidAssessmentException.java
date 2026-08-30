package dev.agenticintegrationpatterns.chapter04.application;

public class InvalidAssessmentException extends RuntimeException {
    public InvalidAssessmentException(String message) {
        super(message);
    }
}
