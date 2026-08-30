package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

public final class AdmittedWorkFingerprint {
    private AdmittedWorkFingerprint() {
    }

    public static String compute(ObjectMapper mapper, AdmittedInvestigation admitted) {
        try {
            var material = new Material(
                    admitted.command(),
                    admitted.trustedContext().tenantId(),
                    admitted.trustedContext().authenticatedWorkloadRef(),
                    admitted.trustedContext().permittedPrincipalRefs().stream().sorted().toList(),
                    admitted.effectiveCapabilities().stream().sorted().toList(),
                    admitted.effectiveLimits(),
                    admitted.replyContract().name());
            return ArtifactDigest.sha256(mapper.writeValueAsBytes(material));
        } catch (Exception failure) {
            throw new IllegalStateException("Admitted work cannot be fingerprinted", failure);
        }
    }

    private record Material(
            dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException command,
            String tenantId,
            String authenticatedWorkloadRef,
            List<String> permittedPrincipalRefs,
            List<String> effectiveCapabilities,
            dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException.ExecutionLimits effectiveLimits,
            String replyContract) {
    }
}
