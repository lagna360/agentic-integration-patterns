package dev.agenticintegrationpatterns.orderdesk.application;

public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message) {
        super(message);
    }
}
