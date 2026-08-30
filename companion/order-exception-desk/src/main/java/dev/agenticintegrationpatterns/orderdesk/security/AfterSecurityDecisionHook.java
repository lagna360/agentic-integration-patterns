package dev.agenticintegrationpatterns.orderdesk.security;

@FunctionalInterface
public interface AfterSecurityDecisionHook {
    void afterDecision(SecurityDecision decision);
}
