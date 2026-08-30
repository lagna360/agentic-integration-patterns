package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

public record KafkaTransportPosition(
        String topic,
        int partition,
        long offset,
        String key) {}
