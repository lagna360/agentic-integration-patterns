package dev.agenticintegrationpatterns.chapter04.application;

public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message) {
        super(message);
    }
}
