package dev.agenticintegrationpatterns.orderdesk.coordination;

import java.time.Instant;

public record EvidenceFinding(
        String evidenceKey,
        String canonicalValue,
        String valueSha256,
        String sourceSystem,
        String sourceVersion,
        Instant observedAt) {
}
