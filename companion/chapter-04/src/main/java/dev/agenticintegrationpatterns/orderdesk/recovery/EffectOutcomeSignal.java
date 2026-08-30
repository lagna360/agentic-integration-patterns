package dev.agenticintegrationpatterns.orderdesk.recovery;

public record EffectOutcomeSignal(
        String tenantId,
        String messageId,
        String planId,
        String effectId,
        String evidenceRef) {
    public EffectOutcomeSignal {
        ResolutionPlanDefinition.require(tenantId, "tenantId", 120);
        ResolutionPlanDefinition.require(messageId, "messageId", 160);
        ResolutionPlanDefinition.require(planId, "planId", 160);
        ResolutionPlanDefinition.require(effectId, "effectId", 160);
        ResolutionPlanDefinition.require(evidenceRef, "evidenceRef", 600);
    }
}
