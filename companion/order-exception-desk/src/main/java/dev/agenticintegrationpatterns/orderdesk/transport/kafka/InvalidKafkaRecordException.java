package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

public final class InvalidKafkaRecordException extends RuntimeException {
    public InvalidKafkaRecordException(String message) {
        super(message);
    }
}
