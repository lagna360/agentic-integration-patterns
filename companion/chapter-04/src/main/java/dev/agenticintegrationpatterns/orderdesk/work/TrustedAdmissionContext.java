package dev.agenticintegrationpatterns.orderdesk.work;

import java.time.Instant;
import java.util.Set;

public record TrustedAdmissionContext(
        String tenantId,
        String authenticatedWorkloadRef,
        Set<String> permittedPrincipalRefs,
        Instant receivedAt) {

    public TrustedAdmissionContext {
        permittedPrincipalRefs = permittedPrincipalRefs == null
                ? null : Set.copyOf(permittedPrincipalRefs);
    }
}
