package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

public record IngressReceipt(Disposition disposition, String runId) {
    public enum Disposition {
        ACCEPTED,
        DUPLICATE_SAME,
        DUPLICATE_CONFLICT
    }
}
