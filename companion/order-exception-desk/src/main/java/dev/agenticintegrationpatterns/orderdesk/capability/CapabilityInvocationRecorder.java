package dev.agenticintegrationpatterns.orderdesk.capability;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public final class CapabilityInvocationRecorder {
    private final CopyOnWriteArrayList<InvocationRecord> records = new CopyOnWriteArrayList<>();

    public void success(CapabilityEvidence evidence) {
        records.add(new InvocationRecord(
                evidence.runId(), evidence.tenantId(), evidence.callId(),
                evidence.capabilityName(), "SUCCEEDED", null, evidence.executedAt(),
                evidence.evidenceId()));
    }

    public void failure(
            CapabilityInvocationRequest request,
            CapabilityGatewayException.Reason reason,
            Instant recordedAt) {
        records.add(new InvocationRecord(
                safeRunId(request), safeTenantId(request), safeCallId(request),
                safeCapability(request), "DENIED_OR_FAILED", reason.name(), recordedAt, null));
    }

    public List<InvocationRecord> records() {
        return List.copyOf(records);
    }

    public void clear() {
        records.clear();
    }

    private static String safeRunId(CapabilityInvocationRequest request) {
        return request == null || request.context() == null || request.context().snapshot() == null
                ? null : request.context().snapshot().runId();
    }

    private static String safeTenantId(CapabilityInvocationRequest request) {
        return request == null || request.context() == null || request.context().snapshot() == null
                ? null : request.context().snapshot().tenantId();
    }

    private static String safeCallId(CapabilityInvocationRequest request) {
        return request == null || request.intent() == null
                ? null : boundedPrintable(request.intent().callId(), 128);
    }

    private static String safeCapability(CapabilityInvocationRequest request) {
        return request == null || request.intent() == null
                ? null : boundedPrintable(request.intent().capabilityName(), 128);
    }

    private static String boundedPrintable(String value, int maximumLength) {
        if (value == null) {
            return null;
        }
        String printable = value.replaceAll("[\\p{Cntrl}]", "?");
        return printable.length() <= maximumLength
                ? printable : printable.substring(0, maximumLength);
    }

    public record InvocationRecord(
            String runId,
            String tenantId,
            String callId,
            String capabilityName,
            String outcome,
            String reason,
            Instant recordedAt,
            String evidenceId) {
    }
}
