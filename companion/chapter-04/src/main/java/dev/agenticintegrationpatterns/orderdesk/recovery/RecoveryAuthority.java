package dev.agenticintegrationpatterns.orderdesk.recovery;

import java.time.Instant;

/** Current, typed authority for one exact recovery effect; never a Boolean flag. */
public record RecoveryAuthority(
        String authorityId,
        String tenantId,
        String planId,
        long planVersion,
        String failedEffectId,
        long failedEffectVersion,
        String effectId,
        String compensatesEffectId,
        String effectType,
        String targetResourceKey,
        String policyRef,
        String reasonCode,
        String evidenceSha256,
        String configurationRef,
        Instant validUntil) {

    public RecoveryAuthority {
        require(authorityId, "authorityId", 160);
        require(tenantId, "tenantId", 120);
        require(planId, "planId", 160);
        if (planVersion < 0) {
            throw new IllegalArgumentException("planVersion must not be negative");
        }
        require(failedEffectId, "failedEffectId", 160);
        if (failedEffectVersion < 1) {
            throw new IllegalArgumentException("failedEffectVersion must be positive");
        }
        require(effectId, "effectId", 160);
        require(compensatesEffectId, "compensatesEffectId", 160);
        require(effectType, "effectType", 80);
        require(targetResourceKey, "targetResourceKey", 320);
        require(policyRef, "policyRef", 240);
        require(reasonCode, "reasonCode", 120);
        require(evidenceSha256, "evidenceSha256", 64);
        if (!evidenceSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("evidenceSha256 must be lowercase SHA-256");
        }
        require(configurationRef, "configurationRef", 240);
        if (validUntil == null) {
            throw new IllegalArgumentException("validUntil is required");
        }
    }

    private static void require(String value, String name, int max) {
        if (value == null || value.isBlank() || value.length() > max) {
            throw new IllegalArgumentException(name + " is missing or too long");
        }
    }
}
