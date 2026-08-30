package dev.agenticintegrationpatterns.orderdesk.history;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** A normalized pointer to a durable fact; it deliberately does not store raw prompts or payloads. */
public record HistoryObservation(
        String tenantId,
        String observationId,
        String sourceStream,
        long sourceSequence,
        String messageId,
        String eventId,
        String payloadSha256,
        String sourceOwner,
        EventClass eventClass,
        String outcomeCode,
        TrustClass trustClass,
        Instant occurredAt,
        List<IdentityLink> identityLinks,
        TraceContext traceContext,
        UsageObservation usageObservation,
        MeasuredUsage measuredUsage,
        String summaryCode,
        String redactionProfile,
        String retentionClass,
        Instant retainUntil) {

    public HistoryObservation {
        requireText(tenantId, "tenantId");
        requireText(observationId, "observationId");
        requireText(sourceStream, "sourceStream");
        if (sourceSequence < 0) throw new IllegalArgumentException("sourceSequence");
        requireSha(payloadSha256, "payloadSha256");
        requireText(sourceOwner, "sourceOwner");
        Objects.requireNonNull(eventClass, "eventClass");
        requireText(outcomeCode, "outcomeCode");
        Objects.requireNonNull(trustClass, "trustClass");
        Objects.requireNonNull(occurredAt, "occurredAt");
        identityLinks = identityLinks == null ? List.of() : List.copyOf(identityLinks);
        if (new HashSet<>(identityLinks).size() != identityLinks.size())
            throw new IllegalArgumentException("duplicate identityLinks");
        requireText(summaryCode, "summaryCode");
        requireText(redactionProfile, "redactionProfile");
        requireText(retentionClass, "retentionClass");
        Objects.requireNonNull(retainUntil, "retainUntil");
        if (!retainUntil.isAfter(occurredAt)) throw new IllegalArgumentException("retainUntil");
    }

    public enum EventClass {
        BUSINESS_LIFECYCLE, POLICY_DECISION, APPROVAL_DECISION, EFFECT_OBSERVATION,
        REMOTE_WORK, MODEL_INVOCATION, TOOL_INVOCATION, MESSAGE_DELIVERY
    }

    public enum TrustClass {
        APPLICATION_RECORDED, TARGET_OBSERVED, PROVIDER_REPORTED, PEER_DECLARED
    }

    public enum IdentityKind {
        CASE, COMMAND, RUN, REMOTE_WORK, PEER_TASK, EVIDENCE, PROPOSAL, APPROVAL,
        EFFECT, ATTEMPT, POLICY, MODEL, MODEL_EXECUTION, INSTRUCTION, TOOL_CALL, MESSAGE
    }

    public record IdentityLink(IdentityKind kind, String value) {
        public IdentityLink {
            Objects.requireNonNull(kind, "kind");
            requireText(value, "value");
        }
    }

    public record TraceContext(String traceId, String spanId) {
        public TraceContext {
            requireW3cId(traceId, 32, "traceId");
            requireW3cId(spanId, 16, "spanId");
        }
    }

    public record UsageObservation(UsageSource source, long tokens, long costMicros) {
        public UsageObservation {
            Objects.requireNonNull(source, "source");
            if (tokens < 0 || costMicros < 0) throw new IllegalArgumentException("usageObservation");
        }
    }

    public enum UsageSource {
        PROVIDER_REPORTED, PEER_DECLARED, LOCAL_COUNTED, BILLING_RECONCILED
    }

    public record MeasuredUsage(long durationMillis, long bytes) {
        public MeasuredUsage {
            if (durationMillis < 0 || bytes < 0) throw new IllegalArgumentException("measuredUsage");
        }
    }

    /** Canonical identity for duplicate/collision handling; the payload digest alone is insufficient. */
    public String calculatedSha256() {
        List<Object> fields = new ArrayList<>();
        fields.add(tenantId);
        fields.add(observationId);
        fields.add(sourceStream);
        fields.add(sourceSequence);
        fields.add(messageId);
        fields.add(eventId);
        fields.add(payloadSha256);
        fields.add(sourceOwner);
        fields.add(eventClass);
        fields.add(outcomeCode);
        fields.add(trustClass);
        fields.add(occurredAt);
        List<IdentityLink> sortedLinks = identityLinks.stream()
                .sorted(Comparator.comparing((IdentityLink link) -> link.kind().name())
                        .thenComparing(IdentityLink::value))
                .toList();
        fields.add(sortedLinks.size());
        for (IdentityLink link : sortedLinks) {
            fields.add(link.kind());
            fields.add(link.value());
        }
        fields.add(traceContext == null ? null : traceContext.traceId());
        fields.add(traceContext == null ? null : traceContext.spanId());
        fields.add(usageObservation == null ? null : usageObservation.source());
        fields.add(usageObservation == null ? null : usageObservation.tokens());
        fields.add(usageObservation == null ? null : usageObservation.costMicros());
        fields.add(measuredUsage == null ? null : measuredUsage.durationMillis());
        fields.add(measuredUsage == null ? null : measuredUsage.bytes());
        fields.add(summaryCode);
        fields.add(redactionProfile);
        fields.add(retentionClass);
        fields.add(retainUntil);
        return ReplayInputManifest.canonicalSha256(fields);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field);
    }

    private static void requireSha(String value, String field) {
        if (value == null || !value.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException(field);
    }

    private static void requireW3cId(String value, int length, String field) {
        if (value == null || !value.matches("[0-9a-f]{" + length + "}")
                || value.chars().allMatch(character -> character == '0'))
            throw new IllegalArgumentException(field);
    }
}
