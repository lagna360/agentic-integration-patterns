package dev.agenticintegrationpatterns.orderdesk.application;

public class MalformedEventException extends RuntimeException {
    public MalformedEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
