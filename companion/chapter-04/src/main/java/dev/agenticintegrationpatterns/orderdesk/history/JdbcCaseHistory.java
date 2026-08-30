package dev.agenticintegrationpatterns.orderdesk.history;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.history.HistoryReceipt.Disposition.COLLISION;
import static dev.agenticintegrationpatterns.orderdesk.history.HistoryReceipt.Disposition.DUPLICATE;
import static dev.agenticintegrationpatterns.orderdesk.history.HistoryReceipt.Disposition.RECORDED;
import static dev.agenticintegrationpatterns.orderdesk.history.HistoryReceipt.QualitySignal.*;

/** Transactional projection of existing durable facts into a typed, queryable case history. */
@Component
public class JdbcCaseHistory {
    private static final Duration FUTURE_TOLERANCE = Duration.ofMinutes(2);

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final OpenTelemetryHistoryProjection telemetry;

    public JdbcCaseHistory(
            JdbcTemplate jdbc, Clock clock, OpenTelemetryHistoryProjection telemetry) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.telemetry = telemetry;
    }

    // tag::record-history-observation[]
    @Transactional
    public HistoryReceipt record(HistoryObservation observation) {
        Instant now = clock.instant();
        String observationSha256 = observation.calculatedSha256();
        List<String> prior = jdbc.queryForList("""
                select observation_sha256 from case_history_observation
                where tenant_id=? and observation_id=?
                """, String.class, observation.tenantId(), observation.observationId());
        if (!prior.isEmpty()) {
            if (Objects.equals(prior.get(0), observationSha256))
                return new HistoryReceipt(observation.tenantId(), observation.observationId(),
                        DUPLICATE, Set.of(HistoryReceipt.QualitySignal.DUPLICATE));
            jdbc.update("""
                    insert into case_history_collision
                    (tenant_id, observation_id, original_sha256, received_sha256, recorded_at)
                    values (?,?,?,?,?)
                    """, observation.tenantId(), observation.observationId(), prior.get(0),
                    observationSha256, now);
            return new HistoryReceipt(observation.tenantId(), observation.observationId(),
                    COLLISION, Set.of(IDENTITY_COLLISION));
        }

        EnumSet<HistoryReceipt.QualitySignal> quality = EnumSet.noneOf(
                HistoryReceipt.QualitySignal.class);
        if (observation.traceContext() == null) quality.add(TRACE_CONTEXT_ABSENT);
        if (observation.usageObservation() != null
                && (observation.usageObservation().source()
                        == HistoryObservation.UsageSource.PROVIDER_REPORTED
                || observation.usageObservation().source()
                        == HistoryObservation.UsageSource.PEER_DECLARED))
            quality.add(DECLARED_USAGE_ONLY);
        if (observation.occurredAt().isAfter(now.plus(FUTURE_TOLERANCE)))
            quality.add(FUTURE_SOURCE_TIME);
        assessSequence(observation, now, quality);

        HistoryObservation.TraceContext trace = observation.traceContext();
        HistoryObservation.UsageObservation usage = observation.usageObservation();
        HistoryObservation.MeasuredUsage measured = observation.measuredUsage();
        jdbc.update("""
                insert into case_history_observation
                (tenant_id, observation_id, source_stream, source_sequence, message_id, event_id,
                 payload_sha256, observation_sha256, source_owner, event_class, outcome_code, trust_class,
                 occurred_at, recorded_at, trace_id, span_id,
                 usage_source, usage_tokens, usage_cost_micros,
                 measured_duration_millis, measured_bytes, summary_code, redaction_profile,
                 retention_class, retain_until)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, observation.tenantId(), observation.observationId(),
                observation.sourceStream(), observation.sourceSequence(), observation.messageId(),
                observation.eventId(), observation.payloadSha256(), observationSha256,
                observation.sourceOwner(),
                observation.eventClass().name(), observation.outcomeCode(),
                observation.trustClass().name(), observation.occurredAt(), now,
                trace == null ? null : trace.traceId(), trace == null ? null : trace.spanId(),
                usage == null ? null : usage.source().name(),
                usage == null ? null : usage.tokens(),
                usage == null ? null : usage.costMicros(),
                measured == null ? null : measured.durationMillis(),
                measured == null ? null : measured.bytes(), observation.summaryCode(),
                observation.redactionProfile(), observation.retentionClass(),
                observation.retainUntil());
        for (HistoryObservation.IdentityLink link : observation.identityLinks()) {
            jdbc.update("""
                    insert into case_history_identity_link
                    (tenant_id, observation_id, identity_kind, identity_value) values (?,?,?,?)
                    """, observation.tenantId(), observation.observationId(),
                    link.kind().name(), link.value());
        }

        emitAfterCommit(observation, Set.copyOf(quality));
        return new HistoryReceipt(observation.tenantId(), observation.observationId(),
                RECORDED, Set.copyOf(quality));
    }
    // end::record-history-observation[]

    public HistoryView view(
            AuthorizedHistoryScope scope, String tenantId, String caseId,
            HistoryView.Purpose purpose) {
        scope.require(tenantId, purpose, clock.instant());
        List<HistoryView.Fact> facts = jdbc.query("""
                select o.observation_id, o.source_sequence, o.event_class, o.outcome_code,
                       o.occurred_at, o.recorded_at, o.trace_id,
                       o.usage_source, o.usage_tokens, o.summary_code
                from case_history_observation o
                join case_history_identity_link l
                  on l.tenant_id=o.tenant_id and l.observation_id=o.observation_id
                where o.tenant_id=? and l.identity_kind='CASE' and l.identity_value=?
                order by o.recorded_at, o.observation_id
                """, (rs, row) -> new HistoryView.Fact(
                        rs.getString("observation_id"), rs.getLong("source_sequence"),
                        rs.getString("event_class"), rs.getString("outcome_code"),
                        rs.getTimestamp("occurred_at").toInstant(),
                        rs.getTimestamp("recorded_at").toInstant(),
                        rs.getString("trace_id") != null,
                        rs.getString("usage_source"),
                        (Long) rs.getObject("usage_tokens"), rs.getString("summary_code")),
                tenantId, caseId);
        Set<String> gaps = new HashSet<>();
        if (facts.stream().anyMatch(fact -> !fact.traceContextPresent()))
            gaps.add("TRACE_COVERAGE_INCOMPLETE");
        Integer completeModelProvenance = jdbc.queryForObject("""
                select count(*) from case_history_observation model_observation
                where model_observation.tenant_id=?
                  and model_observation.event_class='MODEL_INVOCATION'
                  and exists (
                    select 1 from case_history_identity_link case_link
                    where case_link.tenant_id=model_observation.tenant_id
                      and case_link.observation_id=model_observation.observation_id
                      and case_link.identity_kind='CASE' and case_link.identity_value=?)
                  and exists (
                    select 1 from case_history_identity_link model_link
                    where model_link.tenant_id=model_observation.tenant_id
                      and model_link.observation_id=model_observation.observation_id
                      and model_link.identity_kind='MODEL')
                  and exists (
                    select 1 from case_history_identity_link instruction_link
                    where instruction_link.tenant_id=model_observation.tenant_id
                      and instruction_link.observation_id=model_observation.observation_id
                      and instruction_link.identity_kind='INSTRUCTION')
                  and exists (
                    select 1 from case_history_identity_link execution_link
                    where execution_link.tenant_id=model_observation.tenant_id
                      and execution_link.observation_id=model_observation.observation_id
                      and execution_link.identity_kind='MODEL_EXECUTION')
                  and exists (
                    select 1 from case_history_observation accepted_outcome
                    where accepted_outcome.tenant_id=model_observation.tenant_id
                      and accepted_outcome.event_class='BUSINESS_LIFECYCLE'
                      and accepted_outcome.outcome_code='PROPOSAL_AVAILABLE'
                      and exists (
                        select 1 from case_history_identity_link accepted_case
                        where accepted_case.tenant_id=accepted_outcome.tenant_id
                          and accepted_case.observation_id=accepted_outcome.observation_id
                          and accepted_case.identity_kind='CASE'
                          and accepted_case.identity_value=?)
                      and exists (
                        select 1
                        from case_history_identity_link model_run
                        join case_history_identity_link accepted_run
                          on accepted_run.tenant_id=model_run.tenant_id
                         and accepted_run.identity_kind='RUN'
                         and accepted_run.identity_value=model_run.identity_value
                        where model_run.tenant_id=model_observation.tenant_id
                          and model_run.observation_id=model_observation.observation_id
                          and model_run.identity_kind='RUN'
                          and accepted_run.observation_id=accepted_outcome.observation_id)
                      and exists (
                        select 1
                        from case_history_identity_link model_proposal
                        join case_history_identity_link accepted_proposal
                          on accepted_proposal.tenant_id=model_proposal.tenant_id
                         and accepted_proposal.identity_kind='PROPOSAL'
                         and accepted_proposal.identity_value=model_proposal.identity_value
                        where model_proposal.tenant_id=model_observation.tenant_id
                          and model_proposal.observation_id=model_observation.observation_id
                          and model_proposal.identity_kind='PROPOSAL'
                          and accepted_proposal.observation_id=accepted_outcome.observation_id))
                """, Integer.class, tenantId, caseId, caseId);
        if (completeModelProvenance == null || completeModelProvenance == 0)
            gaps.add("MISSING_MODEL_EXECUTION_PROVENANCE");
        return new HistoryView(purpose, tenantId, caseId, List.copyOf(facts), Set.copyOf(gaps));
    }

    private void assessSequence(HistoryObservation observation, Instant now,
            Set<HistoryReceipt.QualitySignal> quality) {
        List<Long> cursor = jdbc.queryForList("""
                select highest_sequence from case_history_stream_cursor
                where tenant_id=? and source_stream=? for update
                """, Long.class, observation.tenantId(), observation.sourceStream());
        if (cursor.isEmpty()) {
            jdbc.update("""
                    insert into case_history_stream_cursor
                    (tenant_id, source_stream, highest_sequence, updated_at) values (?,?,?,?)
                    """, observation.tenantId(), observation.sourceStream(),
                    observation.sourceSequence(), now);
            return;
        }
        long highest = cursor.get(0);
        if (observation.sourceSequence() > highest + 1) quality.add(SEQUENCE_GAP);
        if (observation.sourceSequence() <= highest) quality.add(OUT_OF_ORDER);
        if (observation.sourceSequence() > highest) {
            jdbc.update("""
                    update case_history_stream_cursor set highest_sequence=?, updated_at=?
                    where tenant_id=? and source_stream=?
                    """, observation.sourceSequence(), now,
                    observation.tenantId(), observation.sourceStream());
        }
    }

    private void emitAfterCommit(
            HistoryObservation observation, Set<HistoryReceipt.QualitySignal> quality) {
        Runnable emit = () -> {
            try {
                telemetry.emit(observation, quality);
            } catch (RuntimeException ignored) {
                // Telemetry loss must not change committed business-history projection.
            }
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override public void afterCommit() { emit.run(); }
                    });
        } else {
            emit.run();
        }
    }
}
