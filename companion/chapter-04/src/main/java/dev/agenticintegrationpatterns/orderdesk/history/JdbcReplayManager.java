package dev.agenticintegrationpatterns.orderdesk.history;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Executes only an authorized reconstruction or isolated reevaluation over an exact manifest. */
@Component
public class JdbcReplayManager {
    public static final String AUDIENCE = "order-desk-replay";
    public static final String SERVICE = "service:order-desk-replay";

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final ReplayEvaluator evaluator;
    private final TransactionTemplate transactions;

    public JdbcReplayManager(JdbcTemplate jdbc, Clock clock, ReplayEvaluator evaluator,
            PlatformTransactionManager transactionManager) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.evaluator = evaluator;
        this.transactions = new TransactionTemplate(transactionManager);
        this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    // tag::execute-authorized-replay[]
    public ReplayReceipt execute(AuthorizedReplayScope scope, ReplayCommand command) {
        Claim claim = Objects.requireNonNull(transactions.execute(
                ignored -> validateAndClaim(scope, command)));
        if (!claim.requiresEvaluation()) return claim.receipt();

        try {
            ReplayEvaluator.Result result = evaluate(claim.mode(), claim.manifest());
            return Objects.requireNonNull(transactions.execute(
                    ignored -> finalizeCompleted(command, result)));
        } catch (RuntimeException evaluationOrFinalizationFailure) {
            try {
                return Objects.requireNonNull(transactions.execute(
                        ignored -> finalizeOutcomeUnknown(command)));
            } catch (RuntimeException unknownPersistenceFailure) {
                evaluationOrFinalizationFailure.addSuppressed(unknownPersistenceFailure);
                throw evaluationOrFinalizationFailure;
            }
        }
    }
    // end::execute-authorized-replay[]

    private Claim validateAndClaim(AuthorizedReplayScope scope, ReplayCommand command) {
        Instant now = clock.instant();
        requireScopeEnvelope(scope, command, now);
        String requestSha256 = command.calculatedSha256();

        // The row lock exists only inside this claim transaction. Evaluation happens after commit.
        Authority authority = loadAuthority(command);
        ReplayInputManifest manifest = loadManifest(command.tenantId(), command.manifestId());
        requireCurrentAuthority(scope, command, authority, manifest, now);
        List<ExistingExecution> existing = loadExisting(command);
        if (!existing.isEmpty()) {
            if (!Objects.equals(existing.get(0).requestSha256(), requestSha256))
                throw new IllegalStateException("REPLAY_ID_COLLISION");
            return Claim.existing(existing.get(0).receipt());
        }
        if (!Set.of("RECONSTRUCT", "REEVALUATE").contains(authority.mode()))
            throw new IllegalStateException("UNSUPPORTED_REPLAY_MODE");
        if (!evaluator.isAvailable(authority.mode(), manifest))
            throw new IllegalStateException("PINNED_IMPLEMENTATION_UNAVAILABLE");

        consumeAuthority(command, authority);
        String replayRunId = "replay-run:" + command.replayId();
        jdbc.update("""
                insert into replay_execution
                (tenant_id, replay_id, replay_run_id, source_run_id, authorization_id,
                 manifest_id, manifest_sha256, replay_mode, purpose, execution_state,
                 request_sha256, started_at)
                values (?,?,?,?,?,?,?,?,?,'RUNNING',?,?)
                """, command.tenantId(), command.replayId(), replayRunId,
                manifest.sourceRunId(), command.authorizationId(), manifest.manifestId(),
                manifest.manifestSha256(), authority.mode(), authority.purpose(),
                requestSha256, now);
        return Claim.claimed(new ReplayReceipt(command.tenantId(), command.replayId(), replayRunId,
                manifest.sourceRunId(), authority.mode(), "RUNNING", null, null, Set.of()),
                manifest, authority.mode());
    }

    private ReplayEvaluator.Result evaluate(String mode, ReplayInputManifest manifest) {
        return switch (mode) {
            case "RECONSTRUCT" -> evaluator.reconstruct(manifest);
            case "REEVALUATE" -> evaluator.reevaluate(manifest);
            default -> throw new IllegalStateException("UNSUPPORTED_REPLAY_MODE");
        };
    }

    private ReplayReceipt finalizeCompleted(
            ReplayCommand command, ReplayEvaluator.Result result) {
        int changed = jdbc.update("""
                update replay_execution set execution_state='COMPLETED', result_code=?,
                result_sha256=?, result_gaps=?, completed_at=?
                where tenant_id=? and replay_id=? and execution_state='RUNNING'
                """, result.resultCode().name(), result.sha256(),
                canonicalGaps(result.explicitGaps()), clock.instant(),
                command.tenantId(), command.replayId());
        if (changed != 1) return requireExisting(command).receipt();
        return requireExisting(command).receipt();
    }

    private ReplayReceipt finalizeOutcomeUnknown(ReplayCommand command) {
        jdbc.update("""
                update replay_execution set execution_state='OUTCOME_UNKNOWN', result_code=null,
                result_sha256=null, result_gaps=?, completed_at=?
                where tenant_id=? and replay_id=? and execution_state='RUNNING'
                """, ReplayEvaluator.GapCode.EVALUATION_OUTCOME_UNKNOWN.name(), clock.instant(),
                command.tenantId(), command.replayId());
        return requireExisting(command).receipt();
    }

    private Authority loadAuthority(ReplayCommand command) {
        List<Authority> rows = jdbc.query("""
                select manifest_id, manifest_sha256, replay_mode, purpose, expires_at,
                       actor_ref, authority_state, max_uses, uses_consumed, effect_mode
                from replay_authorization
                where tenant_id=? and authorization_id=?
                for update
                """, (rs, row) -> new Authority(rs.getString("manifest_id"),
                        rs.getString("manifest_sha256"), rs.getString("replay_mode"),
                        rs.getString("purpose"), rs.getTimestamp("expires_at").toInstant(),
                        rs.getString("actor_ref"), rs.getString("authority_state"),
                        rs.getInt("max_uses"),
                        rs.getInt("uses_consumed"), rs.getString("effect_mode")),
                command.tenantId(), command.authorizationId());
        if (rows.isEmpty()) throw new SecurityException("REPLAY_AUTHORITY_NOT_FOUND");
        return rows.get(0);
    }

    private ReplayInputManifest loadManifest(String tenantId, String manifestId) {
        List<ReplayInputManifest> rows = jdbc.query("""
                select tenant_id, manifest_id, source_case_id, source_run_id, as_of_event_id,
                       snapshot_ref, snapshot_sha256, evidence_set_ref, evidence_set_sha256,
                       model_ref, instruction_ref, tool_catalog_ref, policy_ref,
                       configuration_ref, manifest_sha256, retention_state
                from replay_input_manifest where tenant_id=? and manifest_id=?
                """, (rs, row) -> new ReplayInputManifest(
                        rs.getString("tenant_id"), rs.getString("manifest_id"),
                        rs.getString("source_case_id"), rs.getString("source_run_id"),
                        rs.getString("as_of_event_id"), rs.getString("snapshot_ref"),
                        rs.getString("snapshot_sha256"), rs.getString("evidence_set_ref"),
                        rs.getString("evidence_set_sha256"), rs.getString("model_ref"),
                        rs.getString("instruction_ref"), rs.getString("tool_catalog_ref"),
                        rs.getString("policy_ref"), rs.getString("configuration_ref"),
                        rs.getString("manifest_sha256"), rs.getString("retention_state")),
                tenantId, manifestId);
        if (rows.isEmpty()) throw new IllegalStateException("REPLAY_MANIFEST_NOT_FOUND");
        return rows.get(0);
    }

    private void requireCurrentAuthority(AuthorizedReplayScope scope, ReplayCommand command,
            Authority authority, ReplayInputManifest manifest, Instant now) {
        if (!scope.permittedPurposes().contains(authority.purpose()))
            throw new SecurityException("REPLAY_PURPOSE_DENIED");
        if (!Objects.equals(scope.actorRef(), authority.actorRef()))
            throw new SecurityException("REPLAY_ACTOR_MISMATCH");
        if (!Objects.equals(authority.manifestId(), command.manifestId()))
            throw new SecurityException("REPLAY_MANIFEST_NOT_AUTHORIZED");
        if (!Objects.equals(authority.manifestSha256(), manifest.manifestSha256()))
            throw new SecurityException("REPLAY_MANIFEST_DIGEST_MISMATCH");
        if (!Objects.equals(manifest.calculatedSha256(), manifest.manifestSha256()))
            throw new IllegalStateException("RETAINED_INPUT_CHANGED");
        if (!"RETAINED".equals(manifest.retentionState()))
            throw new IllegalStateException("REPLAY_INPUT_UNAVAILABLE");
        if (!"ACTIVE".equals(authority.state()))
            throw new SecurityException("REPLAY_AUTHORITY_NOT_ACTIVE");
        if (!now.isBefore(authority.expiresAt()))
            throw new SecurityException("REPLAY_AUTHORITY_EXPIRED");
        if (!"FORBIDDEN".equals(authority.effectMode()))
            throw new SecurityException("REPLAY_EFFECT_MODE_MUST_BE_FORBIDDEN");
    }

    private void requireScopeEnvelope(
            AuthorizedReplayScope scope, ReplayCommand command, Instant now) {
        if (scope == null) throw new SecurityException("REPLAY_SCOPE_MISSING");
        if (!AUDIENCE.equals(scope.audience())) throw new SecurityException("REPLAY_AUDIENCE_DENIED");
        if (!SERVICE.equals(scope.serviceRef())) throw new SecurityException("REPLAY_SERVICE_DENIED");
        if (!now.isBefore(scope.expiresAt())) throw new SecurityException("REPLAY_SCOPE_EXPIRED");
        if (!scope.permittedTenantIds().contains(command.tenantId()))
            throw new SecurityException("REPLAY_TENANT_DENIED");
    }

    private List<ExistingExecution> loadExisting(ReplayCommand command) {
        return jdbc.query("""
                select tenant_id, replay_id, replay_run_id, source_run_id, replay_mode,
                       execution_state, result_code, result_sha256, result_gaps, request_sha256
                from replay_execution where tenant_id=? and replay_id=?
                """, (rs, row) -> {
                    String resultCode = rs.getString("result_code");
                    ReplayReceipt receipt = new ReplayReceipt(
                            rs.getString("tenant_id"), rs.getString("replay_id"),
                            rs.getString("replay_run_id"), rs.getString("source_run_id"),
                            rs.getString("replay_mode"), rs.getString("execution_state"),
                            resultCode == null ? null
                                    : ReplayEvaluator.ResultCode.valueOf(resultCode),
                            rs.getString("result_sha256"), parseGaps(rs.getString("result_gaps")));
                    return new ExistingExecution(receipt, rs.getString("request_sha256"));
                }, command.tenantId(), command.replayId());
    }

    private ExistingExecution requireExisting(ReplayCommand command) {
        List<ExistingExecution> existing = loadExisting(command);
        if (existing.isEmpty()) throw new IllegalStateException("REPLAY_EXECUTION_NOT_FOUND");
        return existing.get(0);
    }

    private void consumeAuthority(ReplayCommand command, Authority authority) {
        if (authority.usesConsumed() >= authority.maxUses())
            throw new SecurityException("REPLAY_AUTHORITY_USE_EXHAUSTED");
        int changed = jdbc.update("""
                update replay_authorization set uses_consumed=uses_consumed+1
                where tenant_id=? and authorization_id=? and uses_consumed=?
                  and uses_consumed < max_uses
                """, command.tenantId(), command.authorizationId(), authority.usesConsumed());
        if (changed != 1) throw new SecurityException("REPLAY_AUTHORITY_USE_EXHAUSTED");
    }

    private static String canonicalGaps(Set<ReplayEvaluator.GapCode> gaps) {
        return gaps.stream().map(Enum::name).sorted().collect(Collectors.joining(","));
    }

    private static Set<ReplayEvaluator.GapCode> parseGaps(String value) {
        if (value == null || value.isBlank()) return Set.of();
        return Arrays.stream(value.split(","))
                .map(ReplayEvaluator.GapCode::valueOf).collect(Collectors.toUnmodifiableSet());
    }

    private record Authority(
            String manifestId, String manifestSha256, String mode, String purpose,
            Instant expiresAt, String actorRef, String state, int maxUses, int usesConsumed,
            String effectMode) {}

    private record ExistingExecution(ReplayReceipt receipt, String requestSha256) {}

    private record Claim(ReplayReceipt receipt, ReplayInputManifest manifest, String mode,
            boolean requiresEvaluation) {
        private static Claim existing(ReplayReceipt receipt) {
            return new Claim(receipt, null, null, false);
        }

        private static Claim claimed(
                ReplayReceipt receipt, ReplayInputManifest manifest, String mode) {
            return new Claim(receipt, manifest, mode, true);
        }
    }
}
