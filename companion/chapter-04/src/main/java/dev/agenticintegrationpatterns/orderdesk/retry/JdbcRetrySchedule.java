package dev.agenticintegrationpatterns.orderdesk.retry;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;

import static dev.agenticintegrationpatterns.orderdesk.retry.RetryScheduleReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.retry.RetryScheduleReceipt.State.*;

/** A narrow durable schedule and claim boundary, not a general workflow engine. */
@Component
public class JdbcRetrySchedule {
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public JdbcRetrySchedule(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    // tag::durable-retry-schedule[]
    @Transactional
    public RetryScheduleReceipt schedule(
            RetryPolicyRequest request, RetryPolicyDecision decision) {
        if (decision.disposition() != RetryPolicyDecision.Disposition.SCHEDULED) {
            throw new IllegalArgumentException("only an eligible retry may be scheduled");
        }
        String fingerprint = fingerprint(request, decision);
        var existing = find(request.tenantId(), request.scheduleId());
        if (existing.isPresent()) {
            return receipt(existing.get().fingerprint().equals(fingerprint)
                    ? DUPLICATE_SAME : IDENTITY_COLLISION, existing.get());
        }
        Instant now = clock.instant();
        try {
            jdbc.update("""
                    insert into retry_schedule
                    (tenant_id, schedule_id, run_id, operation_key, request_sha256,
                     failure_observation_id, reason_code, effect_id, effect_state,
                     deadline_at, not_before, idempotency_expires_at,
                     minimum_execution_millis, configured_attempt_timeout_millis,
                     settlement_reserve_millis, attempt_timeout_millis,
                     max_attempts, attempts_used,
                     max_tokens, tokens_used, reserved_tokens, max_cost_micros,
                     cost_used_micros, reserved_cost_micros, schedule_state,
                     version, claim_owner, claim_until, created_at, updated_at)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, 'SCHEDULED', 0, null, null, ?, ?)
                    """, request.tenantId(), request.scheduleId(), request.failure().runId(),
                    request.operationKey(), fingerprint, request.failure().observationId(),
                    request.failure().reasonCode(),
                    request.effect() == null ? null : request.effect().effectId(),
                    request.effect() == null ? null : request.effect().state().name(),
                    request.absoluteDeadline(), decision.notBefore(),
                    request.effect() == null ? null : request.effect().idempotencyExpiresAt(),
                    request.minimumUsefulAttemptTimeout().toMillis(),
                    request.configuredAttemptTimeout().toMillis(),
                    request.settlementReserve().toMillis(), decision.attemptTimeout().toMillis(),
                    request.maxAttempts(),
                    request.attemptsUsed(), request.maxTokens(), request.tokensUsed(),
                    request.nextAttemptTokens(), request.maxCostMicros(),
                    request.costUsedMicros(), request.nextAttemptCostMicros(), now, now);
        } catch (DuplicateKeyException concurrentInsert) {
            var winner = find(request.tenantId(), request.scheduleId()).orElseThrow();
            return receipt(winner.fingerprint().equals(fingerprint)
                    ? DUPLICATE_SAME : IDENTITY_COLLISION, winner);
        }
        return receipt(CREATED, find(request.tenantId(), request.scheduleId()).orElseThrow());
    }

    @Transactional
    public Optional<ClaimedRetry> claimDue(
            String tenantId, String scheduleId, String owner, Duration leaseDuration) {
        requireText(tenantId, "tenantId", 120);
        requireText(scheduleId, "scheduleId", 160);
        requireText(owner, "owner", 200);
        if (leaseDuration == null || leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        var current = find(tenantId, scheduleId);
        if (current.isEmpty() || current.get().state() != SCHEDULED) {
            return Optional.empty();
        }
        var row = current.get();
        Instant now = clock.instant();
        long remainingMillis = Duration.between(now, row.deadlineAt()).toMillis()
                - row.settlementReserveMillis();
        long attemptTimeoutMillis = Math.min(row.attemptTimeoutMillis(), Math.min(
                row.configuredAttemptTimeoutMillis(), remainingMillis));
        Instant validUntil = now.plusMillis(attemptTimeoutMillis);
        if (row.notBefore().isAfter(now)
                || attemptTimeoutMillis < row.minimumExecutionMillis()
                || (row.idempotencyExpiresAt() != null
                    && !validUntil.isBefore(row.idempotencyExpiresAt()))) {
            return Optional.empty();
        }
        Instant claimUntil = now.plus(leaseDuration);
        long nextVersion = row.version() + 1;
        int updated = jdbc.update("""
                update retry_schedule
                   set schedule_state='CLAIMED', version=?, claim_owner=?, claim_until=?,
                       attempt_timeout_millis=?,
                       attempts_used=attempts_used+1,
                       tokens_used=tokens_used+reserved_tokens,
                       cost_used_micros=cost_used_micros+reserved_cost_micros,
                       updated_at=?
                 where tenant_id=? and schedule_id=? and schedule_state='SCHEDULED'
                   and version=? and not_before<=? and deadline_at>?
                   and attempts_used<max_attempts
                   and tokens_used+reserved_tokens<=max_tokens
                   and cost_used_micros+reserved_cost_micros<=max_cost_micros
                   and (idempotency_expires_at is null or idempotency_expires_at>?)
                """, nextVersion, owner, claimUntil, attemptTimeoutMillis, now,
                tenantId, scheduleId,
                row.version(), now, validUntil, validUntil);
        if (updated != 1) {
            return Optional.empty();
        }
        var claimed = find(tenantId, scheduleId).orElseThrow();
        return Optional.of(new ClaimedRetry(
                tenantId, scheduleId, claimed.runId(), claimed.operationKey(), owner,
                claimed.version(), claimUntil, Duration.ofMillis(claimed.attemptTimeoutMillis()),
                claimed.attemptsUsed(),
                claimed.tokensUsed(), claimed.costUsedMicros()));
    }
    // end::durable-retry-schedule[]

    @Transactional
    public void consume(ClaimedRetry claim) {
        int updated = jdbc.update("""
                update retry_schedule set schedule_state='CONSUMED', version=version+1,
                       claim_owner=null, claim_until=null, updated_at=?
                 where tenant_id=? and schedule_id=? and schedule_state='CLAIMED'
                   and version=? and claim_owner=?
                """, clock.instant(), claim.tenantId(), claim.scheduleId(),
                claim.version(), claim.owner());
        if (updated != 1) {
            throw new IllegalStateException("retry permit is stale or already consumed");
        }
    }

    public RetryScheduleReceipt current(String tenantId, String scheduleId) {
        return receipt(DUPLICATE_SAME, find(tenantId, scheduleId).orElseThrow(
                () -> new IllegalArgumentException("unknown retry schedule")));
    }

    private Optional<Row> find(String tenantId, String scheduleId) {
        return jdbc.query("""
                select tenant_id, schedule_id, run_id, operation_key, request_sha256,
                       deadline_at, not_before, idempotency_expires_at,
                       minimum_execution_millis, configured_attempt_timeout_millis,
                       settlement_reserve_millis, attempt_timeout_millis,
                       schedule_state, version,
                       attempts_used, tokens_used, cost_used_micros
                  from retry_schedule where tenant_id=? and schedule_id=?
                """, (rs, ignored) -> new Row(
                        rs.getString("tenant_id"), rs.getString("schedule_id"),
                        rs.getString("run_id"), rs.getString("operation_key"),
                        rs.getString("request_sha256"),
                        rs.getObject("deadline_at", Instant.class),
                        rs.getObject("not_before", Instant.class),
                        rs.getObject("idempotency_expires_at", Instant.class),
                        rs.getLong("minimum_execution_millis"),
                        rs.getLong("configured_attempt_timeout_millis"),
                        rs.getLong("settlement_reserve_millis"),
                        rs.getLong("attempt_timeout_millis"),
                        RetryScheduleReceipt.State.valueOf(rs.getString("schedule_state")),
                        rs.getLong("version"), rs.getInt("attempts_used"),
                        rs.getLong("tokens_used"), rs.getLong("cost_used_micros")),
                tenantId, scheduleId).stream().findFirst();
    }

    private static RetryScheduleReceipt receipt(
            RetryScheduleReceipt.Disposition disposition, Row row) {
        return new RetryScheduleReceipt(disposition, row.tenantId(), row.scheduleId(),
                row.state(), row.version(), row.attemptsUsed(), row.tokensUsed(),
                row.costUsedMicros(), row.notBefore());
    }

    private static String fingerprint(
            RetryPolicyRequest request, RetryPolicyDecision decision) {
        String canonical = String.join("\u001f",
                request.tenantId(), request.scheduleId(), request.operationKey(),
                request.failure().runId(), request.failure().observationId(),
                request.failure().reasonCode(),
                request.effect() == null ? "-" : request.effect().effectId(),
                request.effect() == null ? "-" : request.effect().state().name(),
                request.effect() == null ? "-" : Long.toString(request.effect().version()),
                request.effect() == null ? "-" : Integer.toString(request.effect().attemptCount()),
                request.effect() == null ? "-" : nullable(request.effect().targetIdempotencyKey()),
                request.effect() == null ? "-" : nullable(request.effect().idempotencyExpiresAt()),
                request.effect() == null ? "-" : nullable(request.effect().targetReference()),
                request.absoluteDeadline().toString(), decision.notBefore().toString(),
                request.baseBackoff().toString(), request.maximumBackoff().toString(),
                request.configuredAttemptTimeout().toString(),
                request.settlementReserve().toString(),
                request.minimumUsefulAttemptTimeout().toString(),
                Integer.toString(request.maxAttempts()), Integer.toString(request.attemptsUsed()),
                Long.toString(request.maxTokens()), Long.toString(request.tokensUsed()),
                Long.toString(request.nextAttemptTokens()),
                Long.toString(request.maxCostMicros()), Long.toString(request.costUsedMicros()),
                Long.toString(request.nextAttemptCostMicros()));
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void requireText(String value, String field, int length) {
        if (value == null || value.isBlank() || value.length() > length) {
            throw new IllegalArgumentException(field + " is invalid");
        }
    }

    private static String nullable(Object value) {
        return value == null ? "<null>" : value.toString();
    }

    private record Row(
            String tenantId, String scheduleId, String runId, String operationKey,
            String fingerprint, Instant deadlineAt, Instant notBefore,
            Instant idempotencyExpiresAt, long minimumExecutionMillis,
            long configuredAttemptTimeoutMillis, long settlementReserveMillis,
            long attemptTimeoutMillis,
            RetryScheduleReceipt.State state, long version, int attemptsUsed,
            long tokensUsed, long costUsedMicros) { }
}
