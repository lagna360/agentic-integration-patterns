package dev.agenticintegrationpatterns.orderdesk.model;

import java.time.Instant;

public record EvidenceReference(String reference, Instant observedAt, boolean fresh) {
}
