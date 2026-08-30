package dev.agenticintegrationpatterns.chapter04.application;

public class AssessmentGatewayException extends RuntimeException {
    public AssessmentGatewayException(String message, Throwable cause) {
        super(message, cause);
    }
}
