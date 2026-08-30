package dev.agenticintegrationpatterns.orderdesk.process;

@FunctionalInterface
public interface ProcessEventPublisher {
    void publish(ProcessOutboxEvent event);
}
