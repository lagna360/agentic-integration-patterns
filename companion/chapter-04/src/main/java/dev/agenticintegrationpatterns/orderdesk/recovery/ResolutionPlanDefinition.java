package dev.agenticintegrationpatterns.orderdesk.recovery;

import java.time.Instant;
import java.util.List;

public record ResolutionPlanDefinition(
        String tenantId,
        String planId,
        String runId,
        String caseId,
        String proposalId,
        String proposalEventId,
        String recoveryOwner,
        Instant deadlineAt,
        String evidenceSetRef,
        String evidenceSha256,
        String configurationRef,
        long firstEventNumber,
        List<ForwardEffect> effects) {

    public ResolutionPlanDefinition {
        require(tenantId, "tenantId", 120);
        require(planId, "planId", 160);
        require(runId, "runId", 160);
        require(caseId, "caseId", 160);
        require(proposalId, "proposalId", 160);
        require(proposalEventId, "proposalEventId", 240);
        require(recoveryOwner, "recoveryOwner", 240);
        require(evidenceSetRef, "evidenceSetRef", 600);
        require(evidenceSha256, "evidenceSha256", 64);
        require(configurationRef, "configurationRef", 240);
        if (deadlineAt == null || firstEventNumber < 1 || effects == null || effects.size() < 2) {
            throw new IllegalArgumentException("deadline, event sequence, and two effects are required");
        }
        effects = List.copyOf(effects);
    }

    public record ForwardEffect(
            int stepNumber,
            String effectId,
            String effectType,
            String causedByEffectId,
            String targetContractRef,
            String authorityRef,
            Instant authorityValidUntil,
            Reversibility reversibility) {
        public ForwardEffect {
            if (stepNumber < 1 || authorityValidUntil == null || reversibility == null) {
                throw new IllegalArgumentException("complete forward effect metadata is required");
            }
            require(effectId, "effectId", 160);
            require(effectType, "effectType", 80);
            require(targetContractRef, "targetContractRef", 240);
            require(authorityRef, "authorityRef", 600);
        }
    }

    public enum Reversibility { COMPENSATABLE, CORRECTIVE_FORWARD_ONLY, IRREVERSIBLE }

    static void require(String value, String name, int max) {
        if (value == null || value.isBlank() || value.length() > max) {
            throw new IllegalArgumentException(name + " is missing or too long");
        }
    }
}
