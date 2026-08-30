package dev.agenticintegrationpatterns.orderdesk.capability;

import tools.jackson.databind.JsonNode;

public record ToolCallIntent(
        String callId,
        String capabilityName,
        JsonNode arguments) {

    public ToolCallIntent {
        arguments = arguments == null ? null : arguments.deepCopy();
    }

    @Override
    public JsonNode arguments() {
        return arguments == null ? null : arguments.deepCopy();
    }
}
