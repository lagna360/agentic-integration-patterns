package dev.agenticintegrationpatterns.orderdesk.context;

import org.springframework.stereotype.Component;

@Component
public final class ContextPolicy {
    public String selectionPolicyVersion() {
        return "orderdesk-context-selection-v1";
    }

    public String normalizationVersion() {
        return "utf8-json-text-v1";
    }

    public String redactionPolicyVersion() {
        return "email-mask-demo-v1";
    }

    public int maxArtifactBytes() {
        return 4_096;
    }

    public int maxContextBytes() {
        return 8_192;
    }

    public int maxEstimatedTokens() {
        return 2_048;
    }
}
