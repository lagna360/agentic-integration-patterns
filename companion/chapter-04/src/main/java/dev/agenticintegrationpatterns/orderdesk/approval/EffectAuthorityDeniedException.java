package dev.agenticintegrationpatterns.orderdesk.approval;

public final class EffectAuthorityDeniedException extends RuntimeException {
    public EffectAuthorityDeniedException(String message) {
        super(message);
    }
}
