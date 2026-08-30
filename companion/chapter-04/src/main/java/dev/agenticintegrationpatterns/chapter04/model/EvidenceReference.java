package dev.agenticintegrationpatterns.chapter04.model;

import java.time.Instant;

public record EvidenceReference(String reference, Instant observedAt, boolean fresh) {
}
