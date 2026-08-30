package dev.agenticintegrationpatterns.orderdesk.recovery;

/** Explicitly reconsiders one previously retained out-of-order observation. */
public record RedriveEffectOutcome(
        EffectOutcomeSignal original,
        EffectOutcomeSignal redrive) {

    public RedriveEffectOutcome {
        if (original == null || redrive == null
                || !original.tenantId().equals(redrive.tenantId())
                || !original.planId().equals(redrive.planId())
                || !original.effectId().equals(redrive.effectId())
                || original.messageId().equals(redrive.messageId())) {
            throw new IllegalArgumentException(
                    "redrive must use a distinct identity for the same plan effect");
        }
    }
}
