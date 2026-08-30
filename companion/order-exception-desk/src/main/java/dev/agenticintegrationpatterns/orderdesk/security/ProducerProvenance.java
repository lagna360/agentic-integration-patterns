package dev.agenticintegrationpatterns.orderdesk.security;

import java.time.Instant;

public record ProducerProvenance(
        String provenanceRef,
        String messageId,
        String producerWorkloadRef,
        String tenantId,
        String integritySha256,
        Instant verifiedAt,
        Instant expiresAt,
        boolean revoked) {
}
