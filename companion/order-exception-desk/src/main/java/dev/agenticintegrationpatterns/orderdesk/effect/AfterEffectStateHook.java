package dev.agenticintegrationpatterns.orderdesk.effect;

@FunctionalInterface
public interface AfterEffectStateHook {
    void afterStateMutation(
            String tenantId, String effectId, EffectReceipt.State state, long version);
}
