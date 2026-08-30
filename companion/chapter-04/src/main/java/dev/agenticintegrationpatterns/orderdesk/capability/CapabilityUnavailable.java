package dev.agenticintegrationpatterns.orderdesk.capability;

public record CapabilityUnavailable(
        String runId,
        String tenantId,
        String callId,
        CapabilityGatewayException.Reason reason) {
}
