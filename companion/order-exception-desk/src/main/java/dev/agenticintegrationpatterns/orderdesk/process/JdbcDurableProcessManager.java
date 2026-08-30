package dev.agenticintegrationpatterns.orderdesk.process;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static dev.agenticintegrationpatterns.orderdesk.process.ProcessReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.process.RunLease.Purpose.DEADLINE;
import static dev.agenticintegrationpatterns.orderdesk.process.RunLease.Purpose.WORK;

@Component
public class JdbcDurableProcessManager {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final AfterProcessStateHook afterState;
    private final Clock clock;

    public JdbcDurableProcessManager(
            JdbcTemplate jdbc,
            ObjectMapper mapper,
            AfterProcessStateHook afterState,
            Clock clock) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.afterState = afterState;
        this.clock = clock;
    }

    // tag::durable-start-transaction[]
    @Transactional
    public ProcessReceipt start(StartInvestigationRun start) {
        String fingerprint = fingerprint(start);
        Instant processedAt = clock.instant();
        var duplicate = duplicate(start.tenantId(), start.messageId(), fingerprint);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }
        var existingRun = findRun(start.tenantId(), start.runId());
        if (existingRun.isPresent()) {
            insertInbox(start.tenantId(), start.messageId(), start.runId(), fingerprint,
                    OUT_OF_ORDER.name(), processedAt);
            return new ProcessReceipt(OUT_OF_ORDER, start.runId(),
                    existingRun.get().state(), existingRun.get().version());
        }

        insertInbox(start.tenantId(), start.messageId(), start.runId(), fingerprint,
                APPLIED.name(), processedAt);
        jdbc.update("""
                insert into investigation_run
                (tenant_id, run_id, case_id, correlation_id, command_id, plan_version,
                 state, resume_state, completion_decision, deadline_at, next_wake_at,
                 attempt_count, version, fence_token, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, 'WAITING_FOR_EVIDENCE', null, 'NONE', ?, ?,
                        0, 0, 0, ?, ?)
                """, start.tenantId(), start.runId(), start.caseId(), start.correlationId(),
                start.commandId(), start.planVersion(), start.deadlineAt(), processedAt,
                processedAt, processedAt);
        start.expectedWork().stream().sorted().forEach(work -> jdbc.update("""
                insert into investigation_expected_work
                (tenant_id, run_id, work_name, required_work, work_status)
                values (?, ?, ?, ?, 'EXPECTED')
                """, start.tenantId(), start.runId(), work,
                start.requiredWork().contains(work)));
        insertOutbox(start.tenantId(), start.runId(), start.caseId(), 0,
                "InvestigationRunStarted", start, processedAt);
        return new ProcessReceipt(APPLIED, start.runId(),
                RunState.WAITING_FOR_EVIDENCE, 0);
    }
    // end::durable-start-transaction[]

    @Transactional
    public Optional<RunLease> claimNextDue(String owner, Duration leaseDuration) {
        StartInvestigationRun.requireText(owner, "owner");
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        Instant now = clock.instant();
        var candidates = jdbc.query("""
                select tenant_id, run_id, state, version, fence_token, deadline_at
                  from investigation_run
                 where ((state='WAITING_FOR_EVIDENCE' and next_wake_at <= ?)
                     or (state='PAUSED' and deadline_at <= ?))
                   and (lease_until is null or lease_until <= ?)
                 order by next_wake_at, tenant_id, run_id
                 fetch first 32 rows only
                """, (rs, row) -> new ClaimCandidate(
                        rs.getString("tenant_id"), rs.getString("run_id"),
                        RunState.valueOf(rs.getString("state")),
                        rs.getLong("version"), rs.getLong("fence_token"),
                        rs.getObject("deadline_at", Instant.class)), now, now, now);

        for (var candidate : candidates) {
            Instant until = now.plus(leaseDuration);
            int claimed = jdbc.update("""
                    update investigation_run
                       set lease_owner=?, lease_until=?, fence_token=fence_token+1,
                           attempt_count=attempt_count+1, updated_at=?
                     where tenant_id=? and run_id=? and fence_token=?
                       and state=?
                       and ((state='WAITING_FOR_EVIDENCE' and next_wake_at <= ?)
                         or (state='PAUSED' and deadline_at <= ?))
                       and (lease_until is null or lease_until <= ?)
                    """, owner, until, now, candidate.tenantId(), candidate.runId(),
                    candidate.fenceToken(), candidate.state().name(), now, now, now);
            if (claimed == 1) {
                var purpose = now.isBefore(candidate.deadlineAt()) ? WORK : DEADLINE;
                return Optional.of(new RunLease(candidate.tenantId(), candidate.runId(), owner,
                        purpose, candidate.fenceToken() + 1, candidate.version(), until,
                        candidate.deadlineAt()));
            }
        }
        return Optional.empty();
    }

    // tag::fenced-evidence-transition[]
    @Transactional
    public ProcessReceipt applyEvidence(RunLease lease, EvidenceSetClosed evidence) {
        if (!lease.tenantId().equals(evidence.tenantId())
                || !lease.runId().equals(evidence.runId())) {
            throw new IllegalArgumentException("lease and evidence identity differ");
        }
        String fingerprint = fingerprint(evidence);
        Instant processedAt = clock.instant();
        var duplicate = duplicate(evidence.tenantId(), evidence.messageId(), fingerprint);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }

        var run = findRun(evidence.tenantId(), evidence.runId());
        if (run.isEmpty()) {
            insertInbox(evidence.tenantId(), evidence.messageId(), evidence.runId(), fingerprint,
                    UNKNOWN_RUN.name(), processedAt);
            return new ProcessReceipt(UNKNOWN_RUN, evidence.runId(), null, -1);
        }
        var current = run.get();
        if (current.state() != RunState.WAITING_FOR_EVIDENCE) {
            var disposition = current.state() == RunState.PAUSED ? OUT_OF_ORDER : LATE;
            insertInbox(evidence.tenantId(), evidence.messageId(), evidence.runId(), fingerprint,
                    disposition.name(), processedAt);
            return new ProcessReceipt(disposition, evidence.runId(),
                    current.state(), current.version());
        }
        if (lease.purpose() != WORK || !processedAt.isBefore(current.deadlineAt())) {
            insertInbox(evidence.tenantId(), evidence.messageId(), evidence.runId(), fingerprint,
                    DEADLINE_EXCEEDED.name(), processedAt);
            return new ProcessReceipt(DEADLINE_EXCEEDED, evidence.runId(),
                    current.state(), current.version());
        }
        if (!current.leaseMatches(lease, processedAt)) {
            insertInbox(evidence.tenantId(), evidence.messageId(), evidence.runId(), fingerprint,
                    STALE_FENCE.name(), processedAt);
            return new ProcessReceipt(STALE_FENCE, evidence.runId(),
                    current.state(), current.version());
        }
        if (current.version() != lease.version()) {
            throw new ConcurrentRunUpdateException(
                    "run version changed while the leased work was in progress");
        }

        EvidenceSetClosed.Decision validatedDecision;
        try {
            validatedDecision = validateWorkClosure(evidence);
        } catch (InvalidEvidenceClosureException invalid) {
            insertInbox(evidence.tenantId(), evidence.messageId(), evidence.runId(), fingerprint,
                    INVALID_EVIDENCE.name(), processedAt);
            insertRejection(evidence.tenantId(), evidence.messageId(), evidence.runId(),
                    fingerprint, invalid.reason().name(), processedAt);
            return new ProcessReceipt(INVALID_EVIDENCE, evidence.runId(),
                    current.state(), current.version());
        }
        insertInbox(evidence.tenantId(), evidence.messageId(), evidence.runId(), fingerprint,
                APPLIED.name(), processedAt);
        setWorkStatuses(evidence);
        RunState next = validatedDecision == EvidenceSetClosed.Decision.EVIDENCE_READY
                ? RunState.READY_FOR_ASSESSMENT : RunState.REVIEW_REQUIRED;
        long nextVersion = lease.version() + 1;
        // tag::fenced-state-outbox[]
        int updated = jdbc.update("""
                update investigation_run
                   set state=?, completion_decision=?, evidence_set_ref=?,
                       evidence_set_sha256=?, version=?, next_wake_at=null,
                       lease_owner=null, lease_until=null, updated_at=?
                 where tenant_id=? and run_id=?
                   and state='WAITING_FOR_EVIDENCE'
                   and deadline_at > ?
                   and version=? and fence_token=? and lease_owner=?
                """, next.name(), validatedDecision.name(), evidence.evidenceSetRef(),
                evidence.evidenceSetSha256(), nextVersion, processedAt,
                evidence.tenantId(), evidence.runId(), processedAt, lease.version(),
                lease.fenceToken(), lease.owner());
        if (updated != 1) {
            throw new ConcurrentRunUpdateException("run changed while evidence was applied");
        }
        afterState.afterStateMutation(evidence.tenantId(), evidence.runId(), nextVersion);
        insertOutbox(evidence.tenantId(), evidence.runId(), current.caseId(), nextVersion,
                "ParallelEvidenceAccepted", evidence, processedAt);
        // end::fenced-state-outbox[]
        return new ProcessReceipt(APPLIED, evidence.runId(), next, nextVersion);
    }
    // end::fenced-evidence-transition[]

    @Transactional
    public ProcessReceipt stopAtDeadline(String messageId, RunLease lease) {
        Instant firedAt = clock.instant();
        if (firedAt.isBefore(lease.deadlineAt())) {
            throw new IllegalArgumentException("deadline timer fired early");
        }
        String fingerprint = fingerprint(List.of(messageId, lease.tenantId(), lease.runId(),
                lease.fenceToken(), lease.deadlineAt(), lease.purpose()));
        var duplicate = duplicate(lease.tenantId(), messageId, fingerprint);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }
        var run = findRun(lease.tenantId(), lease.runId());
        if (run.isEmpty()) {
            insertInbox(lease.tenantId(), messageId, lease.runId(), fingerprint,
                    UNKNOWN_RUN.name(), firedAt);
            return new ProcessReceipt(UNKNOWN_RUN, lease.runId(), null, -1);
        }
        var current = run.get();
        if (current.state() != RunState.WAITING_FOR_EVIDENCE
                && current.state() != RunState.PAUSED) {
            insertInbox(lease.tenantId(), messageId, lease.runId(), fingerprint, LATE.name(), firedAt);
            return new ProcessReceipt(LATE, lease.runId(), current.state(), current.version());
        }
        if (!current.leaseMatches(lease, firedAt)) {
            insertInbox(lease.tenantId(), messageId, lease.runId(), fingerprint,
                    STALE_FENCE.name(), firedAt);
            return new ProcessReceipt(STALE_FENCE, lease.runId(), current.state(), current.version());
        }
        if (lease.purpose() != DEADLINE) {
            throw new IllegalArgumentException("work lease cannot perform deadline transition");
        }
        if (current.version() != lease.version()) {
            throw new ConcurrentRunUpdateException(
                    "run version changed before the deadline transition");
        }
        insertInbox(lease.tenantId(), messageId, lease.runId(), fingerprint, APPLIED.name(), firedAt);
        long nextVersion = lease.version() + 1;
        int updated = jdbc.update("""
                update investigation_run
                   set state='STOPPED', completion_decision='DEADLINE_EXCEEDED',
                       version=?, next_wake_at=null, lease_owner=null, lease_until=null, updated_at=?
                 where tenant_id=? and run_id=?
                   and state in ('WAITING_FOR_EVIDENCE', 'PAUSED')
                   and version=? and fence_token=? and lease_owner=?
                """, nextVersion, firedAt, lease.tenantId(), lease.runId(), current.version(),
                lease.fenceToken(), lease.owner());
        if (updated != 1) {
            throw new ConcurrentRunUpdateException("run changed while deadline was applied");
        }
        afterState.afterStateMutation(lease.tenantId(), lease.runId(), nextVersion);
        insertOutbox(lease.tenantId(), lease.runId(), current.caseId(), nextVersion,
                "InvestigationRunStopped", java.util.Map.of(
                        "reason", "DEADLINE_EXCEEDED", "deadlineAt", lease.deadlineAt()), firedAt);
        return new ProcessReceipt(APPLIED, lease.runId(), RunState.STOPPED, nextVersion);
    }

    @Transactional
    public ProcessReceipt pause(String messageId, String tenantId, String runId) {
        return changeOperatorState(messageId, tenantId, runId, clock.instant(),
                RunState.WAITING_FOR_EVIDENCE, RunState.PAUSED, "InvestigationRunPaused",
                java.util.Map.of("state", RunState.PAUSED.name()));
    }

    @Transactional
    public ProcessReceipt resume(String messageId, String tenantId, String runId) {
        Instant processedAt = clock.instant();
        var run = findRun(tenantId, runId);
        if (run.isEmpty()) {
            return changeOperatorState(messageId, tenantId, runId, processedAt,
                    RunState.PAUSED, RunState.WAITING_FOR_EVIDENCE,
                    "InvestigationRunResumed",
                    java.util.Map.of("state", RunState.WAITING_FOR_EVIDENCE.name()));
        }
        if (!processedAt.isBefore(run.get().deadlineAt())) {
            return changeOperatorState(messageId, tenantId, runId, processedAt,
                    RunState.PAUSED, RunState.STOPPED, "InvestigationRunStopped",
                    java.util.Map.of("state", RunState.STOPPED.name(),
                            "reason", "DEADLINE_EXCEEDED",
                            "deadlineAt", run.get().deadlineAt()));
        }
        return changeOperatorState(messageId, tenantId, runId, processedAt,
                RunState.PAUSED, RunState.WAITING_FOR_EVIDENCE,
                "InvestigationRunResumed",
                java.util.Map.of("state", RunState.WAITING_FOR_EVIDENCE.name()));
    }

    private ProcessReceipt changeOperatorState(
            String messageId, String tenantId, String runId, Instant receivedAt,
            RunState expected, RunState next, String eventType,
            java.util.Map<String, Object> eventPayload) {
        String fingerprint = fingerprint(List.of(messageId, tenantId, runId, eventType));
        var duplicate = duplicate(tenantId, messageId, fingerprint);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }
        var run = findRun(tenantId, runId);
        if (run.isEmpty()) {
            insertInbox(tenantId, messageId, runId, fingerprint, UNKNOWN_RUN.name(), receivedAt);
            return new ProcessReceipt(UNKNOWN_RUN, runId, null, -1);
        }
        var current = run.get();
        if (current.state() != expected) {
            insertInbox(tenantId, messageId, runId, fingerprint, OUT_OF_ORDER.name(), receivedAt);
            return new ProcessReceipt(OUT_OF_ORDER, runId, current.state(), current.version());
        }
        insertInbox(tenantId, messageId, runId, fingerprint, APPLIED.name(), receivedAt);
        long nextVersion = current.version() + 1;
        Instant nextWake = next == RunState.WAITING_FOR_EVIDENCE ? receivedAt : null;
        int updated = jdbc.update("""
                update investigation_run
                   set state=?, resume_state=?, completion_decision=?, version=?, next_wake_at=?,
                       lease_owner=null, lease_until=null, updated_at=?
                 where tenant_id=? and run_id=? and version=? and state=?
                """, next.name(), next == RunState.PAUSED ? expected.name() : null,
                next == RunState.STOPPED ? "DEADLINE_EXCEEDED" : "NONE",
                nextVersion, nextWake, receivedAt, tenantId, runId, current.version(), expected.name());
        if (updated != 1) {
            throw new ConcurrentRunUpdateException("run changed during operator transition");
        }
        afterState.afterStateMutation(tenantId, runId, nextVersion);
        insertOutbox(tenantId, runId, current.caseId(), nextVersion,
                eventType, eventPayload, receivedAt);
        return new ProcessReceipt(APPLIED, runId, next, nextVersion);
    }

    private Optional<ProcessReceipt> duplicate(
            String tenantId, String messageId, String fingerprint) {
        var matches = jdbc.query("""
                select run_id, payload_fingerprint, disposition
                  from process_message_inbox where tenant_id=? and message_id=?
                """, (rs, row) -> new ExistingMessage(
                        rs.getString("run_id"), rs.getString("payload_fingerprint"),
                        rs.getString("disposition")), tenantId, messageId);
        if (matches.isEmpty()) {
            return Optional.empty();
        }
        var existing = matches.get(0);
        var run = findRun(tenantId, existing.runId());
        if (existing.fingerprint().equals(fingerprint)) {
            return Optional.of(new ProcessReceipt(DUPLICATE_SAME, existing.runId(),
                    run.map(RunRow::state).orElse(null), run.map(RunRow::version).orElse(-1L)));
        }
        jdbc.update("""
                insert into process_message_rejection
                (tenant_id, message_id, run_id, payload_fingerprint, reason, rejected_at)
                values (?, ?, ?, ?, 'MESSAGE_ID_CONTENT_COLLISION', current_timestamp)
                """, tenantId, messageId, existing.runId(), fingerprint);
        return Optional.of(new ProcessReceipt(MESSAGE_ID_COLLISION, existing.runId(),
                run.map(RunRow::state).orElse(null), run.map(RunRow::version).orElse(-1L)));
    }

    private EvidenceSetClosed.Decision validateWorkClosure(EvidenceSetClosed evidence) {
        var expected = jdbc.query("""
                select work_name, required_work from investigation_expected_work
                 where tenant_id=? and run_id=? order by work_name
                """, (rs, row) -> new ExpectedWork(
                        rs.getString("work_name"), rs.getBoolean("required_work")),
                evidence.tenantId(), evidence.runId());
        var reported = new java.util.HashSet<>(evidence.succeededWork());
        reported.addAll(evidence.unavailableWork());
        reported.addAll(evidence.missingWork());
        var expectedNames = expected.stream().map(ExpectedWork::name)
                .collect(java.util.stream.Collectors.toSet());
        if (!expectedNames.equals(reported)) {
            throw new InvalidEvidenceClosureException(
                    InvalidEvidenceClosureException.Reason.EXPECTED_WORK_MISMATCH,
                    "evidence closure does not match persisted expected work");
        }
        var required = expected.stream().filter(ExpectedWork::required)
                .map(ExpectedWork::name).collect(java.util.stream.Collectors.toSet());
        boolean requiredSatisfied = evidence.succeededWork().containsAll(required);
        boolean incomplete = !evidence.missingWork().isEmpty()
                || !evidence.unavailableWork().isEmpty();

        boolean valid = switch (evidence.reason()) {
            case ALL_EXPECTED_EVIDENCE -> !incomplete && requiredSatisfied
                    && evidence.decision() == EvidenceSetClosed.Decision.EVIDENCE_READY;
            case OPTIONAL_EVIDENCE_MISSING -> incomplete && requiredSatisfied
                    && evidence.decision() == EvidenceSetClosed.Decision.EVIDENCE_READY;
            case REQUIRED_EVIDENCE_MISSING -> !requiredSatisfied
                    && evidence.decision() == EvidenceSetClosed.Decision.REVIEW_REQUIRED;
            case EVIDENCE_CONFLICT -> evidence.decision()
                    == EvidenceSetClosed.Decision.REVIEW_REQUIRED;
        };
        if (!valid) {
            throw new InvalidEvidenceClosureException(
                    InvalidEvidenceClosureException.Reason.REQUIRED_WORK_POLICY_CONTRADICTION,
                    "evidence decision contradicts persisted required-work policy or reason");
        }
        return evidence.decision();
    }

    private void setWorkStatuses(EvidenceSetClosed evidence) {
        evidence.succeededWork().forEach(work -> setWorkStatus(evidence, work, "SUCCEEDED"));
        evidence.unavailableWork().forEach(work -> setWorkStatus(evidence, work, "UNAVAILABLE"));
        evidence.missingWork().forEach(work -> setWorkStatus(evidence, work, "MISSING"));
    }

    private void setWorkStatus(EvidenceSetClosed evidence, String work, String status) {
        jdbc.update("""
                update investigation_expected_work set work_status=?
                 where tenant_id=? and run_id=? and work_name=?
                """, status, evidence.tenantId(), evidence.runId(), work);
    }

    private Optional<RunRow> findRun(String tenantId, String runId) {
        return jdbc.query("""
                select case_id, state, version, deadline_at, lease_owner, lease_until, fence_token
                  from investigation_run where tenant_id=? and run_id=?
                """, (rs, row) -> new RunRow(
                        rs.getString("case_id"), RunState.valueOf(rs.getString("state")),
                        rs.getLong("version"), rs.getObject("deadline_at", Instant.class),
                        rs.getString("lease_owner"), rs.getObject("lease_until", Instant.class),
                        rs.getLong("fence_token")), tenantId, runId).stream().findFirst();
    }

    private void insertInbox(
            String tenantId, String messageId, String runId, String fingerprint,
            String disposition, Instant receivedAt) {
        jdbc.update("""
                insert into process_message_inbox
                (tenant_id, message_id, run_id, payload_fingerprint, disposition, first_received_at)
                values (?, ?, ?, ?, ?, ?)
                """, tenantId, messageId, runId, fingerprint, disposition, receivedAt);
    }

    private void insertRejection(
            String tenantId, String messageId, String runId, String fingerprint,
            String reason, Instant rejectedAt) {
        jdbc.update("""
                insert into process_message_rejection
                (tenant_id, message_id, run_id, payload_fingerprint, reason, rejected_at)
                values (?, ?, ?, ?, ?, ?)
                """, tenantId, messageId, runId, fingerprint, reason, rejectedAt);
    }

    private void insertOutbox(
            String tenantId, String runId, String caseId, long version,
            String eventType, Object payload, Instant createdAt) {
        String eventId = "evt-" + UUID.nameUUIDFromBytes(
                (tenantId + "|" + runId + "|" + version + "|" + eventType)
                        .getBytes(StandardCharsets.UTF_8));
        try {
            jdbc.update("""
                    insert into process_outbox
                    (event_id, tenant_id, run_id, case_id, aggregate_version,
                     event_type, event_payload, created_at)
                    values (?, ?, ?, ?, ?, ?, ?, ?)
                    """, eventId, tenantId, runId, caseId, version,
                    eventType, mapper.writeValueAsString(canonicalValue(payload)), createdAt);
        } catch (Exception exception) {
            throw new IllegalStateException("process event could not be serialized", exception);
        }
    }

    private String fingerprint(Object value) {
        try {
            byte[] canonical = mapper.writeValueAsBytes(canonicalValue(value));
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(canonical));
        } catch (Exception exception) {
            throw new IllegalStateException("process message could not be fingerprinted", exception);
        }
    }

    private Object canonicalValue(Object value) {
        if (value instanceof StartInvestigationRun start) {
            return new CanonicalStart(
                    start.messageId(), start.tenantId(), start.runId(), start.caseId(),
                    start.correlationId(), start.commandId(), start.planVersion(),
                    start.deadlineAt(), start.expectedWork().stream().sorted().toList(),
                    start.requiredWork().stream().sorted().toList(), start.receivedAt());
        }
        if (value instanceof EvidenceSetClosed evidence) {
            return new CanonicalEvidence(
                    evidence.messageId(), evidence.tenantId(), evidence.runId(),
                    evidence.evidenceSetRef(), evidence.evidenceSetSha256(), evidence.decision(),
                    evidence.reason(),
                    evidence.succeededWork().stream().sorted().toList(),
                    evidence.unavailableWork().stream().sorted().toList(),
                    evidence.missingWork().stream().sorted().toList(), evidence.receivedAt());
        }
        return value;
    }

    private record ExistingMessage(String runId, String fingerprint, String disposition) { }
    private record ClaimCandidate(
            String tenantId, String runId, RunState state, long version,
            long fenceToken, Instant deadlineAt) { }
    private record CanonicalStart(
            String messageId, String tenantId, String runId, String caseId,
            String correlationId, String commandId, String planVersion,
            Instant deadlineAt, List<String> expectedWork, List<String> requiredWork,
            Instant receivedAt) { }
    private record CanonicalEvidence(
            String messageId, String tenantId, String runId, String evidenceSetRef,
            String evidenceSetSha256, EvidenceSetClosed.Decision decision,
            EvidenceSetClosed.Reason reason,
            List<String> succeededWork, List<String> unavailableWork,
            List<String> missingWork, Instant receivedAt) { }
    private record ExpectedWork(String name, boolean required) { }
    private record RunRow(
            String caseId, RunState state, long version, Instant deadlineAt,
            String leaseOwner, Instant leaseUntil, long fenceToken) {
        boolean leaseMatches(RunLease lease, Instant at) {
            return fenceToken == lease.fenceToken()
                    && lease.owner().equals(leaseOwner)
                    && leaseUntil != null && leaseUntil.isAfter(at);
        }
    }

    public static final class ConcurrentRunUpdateException extends RuntimeException {
        public ConcurrentRunUpdateException(String message) {
            super(message);
        }
    }
}
