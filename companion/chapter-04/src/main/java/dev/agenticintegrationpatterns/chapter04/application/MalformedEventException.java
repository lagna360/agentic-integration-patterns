package dev.agenticintegrationpatterns.chapter04.application;

public class MalformedEventException extends RuntimeException {
    public MalformedEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
