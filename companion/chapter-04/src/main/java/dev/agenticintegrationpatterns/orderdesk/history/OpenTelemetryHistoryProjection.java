package dev.agenticintegrationpatterns.orderdesk.history;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.stereotype.Component;

import java.util.Set;

/** Best-effort telemetry emitted only after the durable observation has committed. */
@Component
public class OpenTelemetryHistoryProjection {
    private static final Set<String> METRIC_KEYS =
            Set.of("stage", "outcome", "participant.kind", "quality");
    private static final Set<String> METRIC_OUTCOMES = Set.of(
            "success", "waiting", "partial", "denied", "failed", "unknown",
            "late", "cancelled", "contained", "other");

    private final Tracer tracer;
    private final LongCounter observations;

    public OpenTelemetryHistoryProjection() {
        this(OpenTelemetry.noop());
    }

    OpenTelemetryHistoryProjection(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("orderdesk.history", "1.0");
        this.observations = openTelemetry.getMeter("orderdesk.history")
                .counterBuilder("orderdesk.history.observations").build();
    }

    // tag::bounded-telemetry-projection[]
    public void emit(HistoryObservation observation, Set<HistoryReceipt.QualitySignal> quality) {
        Span span = tracer.spanBuilder("history." + observation.eventClass().name().toLowerCase())
                .startSpan();
        try {
            // High-cardinality identities are diagnostic span attributes, never metric dimensions.
            span.setAttribute("orderdesk.observation.id", observation.observationId());
            observation.identityLinks().stream()
                    .filter(link -> link.kind() == HistoryObservation.IdentityKind.RUN
                            || link.kind() == HistoryObservation.IdentityKind.EFFECT)
                    .forEach(link -> span.setAttribute(
                            "orderdesk." + link.kind().name().toLowerCase() + ".id",
                            link.value()));

            String participant = switch (observation.trustClass()) {
                case PEER_DECLARED -> "remote-peer";
                case PROVIDER_REPORTED -> "model-provider";
                case TARGET_OBSERVED -> "effect-target";
                case APPLICATION_RECORDED -> "application";
            };
            String qualityCode = quality.isEmpty() ? "complete"
                    : quality.size() == 1
                    ? quality.iterator().next().name().toLowerCase()
                    : "multiple";
            observations.add(1, metricAttributes(
                    observation.eventClass().name().toLowerCase(),
                    observation.outcomeCode(), participant, qualityCode));
        } finally {
            span.end();
        }
    }
    // end::bounded-telemetry-projection[]

    static Attributes metricAttributes(
            String stage, String outcome, String participantKind, String quality) {
        return Attributes.of(
                AttributeKey.stringKey("stage"), stage,
                AttributeKey.stringKey("outcome"), boundedOutcome(outcome),
                AttributeKey.stringKey("participant.kind"), participantKind,
                AttributeKey.stringKey("quality"), quality);
    }

    static Set<String> metricKeys() {
        return METRIC_KEYS;
    }

    static Set<String> metricOutcomes() {
        return METRIC_OUTCOMES;
    }

    static String boundedOutcome(String outcomeCode) {
        if (METRIC_OUTCOMES.contains(outcomeCode)) return outcomeCode;
        return switch (outcomeCode) {
            case "RECORDED", "COMPLETED", "PROPOSAL_AVAILABLE", "APPROVED", "COMPENSATED" ->
                    "success";
            case "WAITING", "WAITING_FOR_INPUT", "WAITING_FOR_PEER_AUTHORIZATION" ->
                    "waiting";
            case "PARTIAL" -> "partial";
            case "DENIED", "REJECTED", "EXPIRED" -> "denied";
            case "FAILED" -> "failed";
            case "UNKNOWN" -> "unknown";
            case "LATE" -> "late";
            case "CANCELLED", "TIMED_OUT" -> "cancelled";
            case "CONTAINED" -> "contained";
            default -> "other";
        };
    }
}
