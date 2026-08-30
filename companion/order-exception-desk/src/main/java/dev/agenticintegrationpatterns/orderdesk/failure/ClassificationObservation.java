package dev.agenticintegrationpatterns.orderdesk.failure;

import java.time.Instant;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A trusted, internal observation produced inside an identified run by a
 * gateway, adapter, reducer, or process policy. This is not an external or
 * model-writable wire contract.
 */
public record ClassificationObservation(
        String tenantId,
        String runId,
        String observationId,
        Stage stage,
        Kind kind,
        String reasonCode,
        String evidenceRef,
        Instant observedAt) {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}");
    private static final Pattern REASON_CODE = Pattern.compile("[A-Z][A-Z0-9_]{0,63}");

    public ClassificationObservation {
        tenantId = requireIdentifier("tenantId", tenantId);
        runId = requireIdentifier("runId", runId);
        observationId = requireIdentifier("observationId", observationId);
        stage = Objects.requireNonNull(stage, "stage");
        kind = Objects.requireNonNull(kind, "kind");
        reasonCode = requireReasonCode(reasonCode);
        evidenceRef = requireIdentifier("evidenceRef", evidenceRef);
        observedAt = Objects.requireNonNull(observedAt, "observedAt");

        if (kind == Kind.EXTERNAL_OUTCOME_UNKNOWN && stage != Stage.EXTERNAL_EFFECT) {
            throw new IllegalArgumentException(
                    "EXTERNAL_OUTCOME_UNKNOWN is valid only at the EXTERNAL_EFFECT stage");
        }
        if (kind == Kind.INSUFFICIENT_EVIDENCE && stage != Stage.EVIDENCE_AGGREGATION) {
            throw new IllegalArgumentException(
                    "INSUFFICIENT_EVIDENCE is valid only at the EVIDENCE_AGGREGATION stage");
        }
    }

    private static String requireIdentifier(String field, String value) {
        if (value == null || !IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " is not a bounded identifier");
        }
        return value;
    }

    private static String requireReasonCode(String value) {
        if (value == null || !REASON_CODE.matcher(value).matches()) {
            throw new IllegalArgumentException("reasonCode is not a bounded code");
        }
        return value;
    }

    public enum Stage {
        CONTEXT_ENRICHMENT,
        CAPABILITY_GATEWAY,
        EVIDENCE_AGGREGATION,
        PROCESS_MANAGER,
        EXTERNAL_EFFECT
    }

    public enum Kind {
        INVALID_WORK,
        RESULT_CONTRACT_INVALID,
        POLICY_DENIED,
        DEPENDENCY_UNAVAILABLE,
        DEADLINE_EXCEEDED,
        INSUFFICIENT_EVIDENCE,
        EXTERNAL_OUTCOME_UNKNOWN
    }
}
