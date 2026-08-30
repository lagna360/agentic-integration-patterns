package dev.agenticintegrationpatterns.orderdesk.security;

import java.time.Instant;

public record SecuredResourceSnapshot(
        String resourceRef,
        String tenantId,
        String subjectRef,
        String delegationAuthorityRef,
        String authorityRef,
        String planId,
        long planVersion,
        long effectVersion,
        State state,
        String targetAccountRef,
        String targetTenantId,
        Instant authorityValidUntil,
        boolean authorityRevoked) {

    public enum State { READY_FOR_SECURITY_ADMISSION, TERMINAL, OBSERVATION_REQUIRED }
}
