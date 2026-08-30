package dev.agenticintegrationpatterns.orderdesk.history;

import dev.agenticintegrationpatterns.orderdesk.OrderExceptionApplication;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.history.HistoryObservation.EventClass.*;
import static dev.agenticintegrationpatterns.orderdesk.history.HistoryObservation.IdentityKind.*;
import static dev.agenticintegrationpatterns.orderdesk.history.HistoryObservation.TrustClass.*;
import static dev.agenticintegrationpatterns.orderdesk.history.HistoryReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.history.HistoryReceipt.QualitySignal.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(HistoryAndReplayTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class HistoryAndReplayTest {
    private static final Instant NOW = Instant.parse("2026-08-27T20:00:00Z");
    private static final String TENANT = "tenant-ca";
    private static final String CASE = "case-d5a30e20-f10b-38ca-9198-4834746bd37b";
    private static final String RUN = "run-4c52e781-0838-35ee-84cc-7e59c537ad9c";
    private static final String SHA_A = "a".repeat(64);
    private static final String SHA_B = "b".repeat(64);
    private static final String TRACE_ID = "0123456789abcdef0123456789abcdef";
    private static final String SPAN_ID = "0123456789abcdef";

    @Autowired ProducerTemplate producer;
    @Autowired JdbcTemplate jdbc;
    @Autowired JdbcCaseHistory history;
    @Autowired CountingReplayEvaluator evaluator;
    @Autowired TestClock clock;
    @Autowired FixtureAuthorizedReplayScopeProvider replayScopes;

    @BeforeEach
    void reset() {
        jdbc.update("delete from replay_execution");
        jdbc.update("delete from replay_authorization");
        jdbc.update("delete from replay_input_manifest");
        jdbc.update("delete from case_history_collision");
        jdbc.update("delete from case_history_identity_link");
        jdbc.update("delete from case_history_observation");
        jdbc.update("delete from case_history_stream_cursor");
        clock.set(NOW);
        evaluator.reset();
        replayScopes.reset();
    }

    @Test
    // tag::identity-not-trace-test[]
    void manyBusinessIdentitiesRemainDistinctFromOptionalTraceContext() {
        HistoryReceipt receipt = record(observation("obs-19-01", 1, SHA_A,
                new HistoryObservation.TraceContext(TRACE_ID, SPAN_ID),
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE),
                        link(RUN, HistoryObservation.IdentityKind.RUN),
                        link("msg-evidence-018", HistoryObservation.IdentityKind.MESSAGE),
                        link("proposal-019491", HistoryObservation.IdentityKind.PROPOSAL))));

        assertThat(receipt.disposition()).isEqualTo(RECORDED);
        assertThat(jdbc.queryForObject("select count(*) from case_history_identity_link",
                Integer.class)).isEqualTo(4);
        assertThat(jdbc.queryForObject("select trace_id from case_history_observation",
                String.class)).isEqualTo(TRACE_ID);
    }
    // end::identity-not-trace-test[]

    @Test
    void traceContextAcceptsOnlyLowercaseNonZeroW3cIdentifiers() {
        assertThatThrownBy(() -> new HistoryObservation.TraceContext(
                "0123456789ABCDEF0123456789ABCDEF", SPAN_ID))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("traceId");
        assertThatThrownBy(() -> new HistoryObservation.TraceContext("0".repeat(32), SPAN_ID))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("traceId");
        assertThatThrownBy(() -> new HistoryObservation.TraceContext(TRACE_ID, "0".repeat(16)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("spanId");
        assertThatThrownBy(() -> new HistoryObservation.TraceContext(TRACE_ID, "abcd"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("spanId");
    }

    @Test
    void absentTraceDoesNotEraseDurableFactAndViewNamesTheGap() {
        HistoryReceipt receipt = record(observation("obs-19-no-span", 1, SHA_A, null,
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE))));
        HistoryView view = history.view(scope(HistoryView.Purpose.OPERATIONS), TENANT, CASE,
                HistoryView.Purpose.OPERATIONS);

        assertThat(receipt.qualitySignals()).contains(TRACE_CONTEXT_ABSENT);
        assertThat(view.facts()).hasSize(1);
        assertThat(view.explicitGaps()).contains("TRACE_COVERAGE_INCOMPLETE",
                "MISSING_MODEL_EXECUTION_PROVENANCE");
    }

    @Test
    void duplicateIsAbsorbedButChangedContentPreservesOriginalAndRecordsCollision() {
        HistoryObservation first = observation("obs-19-dup", 1, SHA_A, null,
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE)));
        assertThat(record(first).disposition()).isEqualTo(RECORDED);
        assertThat(record(first).disposition()).isEqualTo(HistoryReceipt.Disposition.DUPLICATE);
        assertThat(record(observation("obs-19-dup", 1, SHA_B, null,
                first.identityLinks())).disposition()).isEqualTo(COLLISION);

        assertThat(jdbc.queryForObject("select payload_sha256 from case_history_observation",
                String.class)).isEqualTo(SHA_A);
        assertThat(jdbc.queryForObject("select count(*) from case_history_collision",
                Integer.class)).isOne();
    }

    @Test
    void changedObservationMetadataCollidesEvenWhenThePayloadDigestIsReused() {
        HistoryObservation first = observation("obs-19-metadata", 1, SHA_A, null,
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE)));
        assertThat(record(first).disposition()).isEqualTo(RECORDED);
        HistoryObservation altered = new HistoryObservation(
                first.tenantId(), first.observationId(), first.sourceStream(),
                first.sourceSequence(), first.messageId(), first.eventId(), first.payloadSha256(),
                first.sourceOwner(), first.eventClass(), "FAILED", first.trustClass(),
                first.occurredAt(), first.identityLinks(), first.traceContext(),
                first.usageObservation(), first.measuredUsage(), first.summaryCode(),
                first.redactionProfile(), first.retentionClass(), first.retainUntil());

        assertThat(record(altered).disposition()).isEqualTo(COLLISION);
        assertThat(jdbc.queryForObject("select outcome_code from case_history_observation",
                String.class)).isEqualTo("RECORDED");
        assertThat(jdbc.queryForObject("select count(*) from case_history_collision",
                Integer.class)).isOne();
    }

    @Test
    void sourceSequenceDetectsBoundedGapAndOutOfOrderWithoutUsingWallClockAsCausation() {
        record(observation("obs-19-s1", 1, SHA_A, null, List.of()));
        HistoryReceipt gap = record(observation("obs-19-s3", 3, SHA_A, null, List.of()));
        HistoryReceipt late = record(observation("obs-19-s2", 2, SHA_A, null, List.of()));

        assertThat(gap.qualitySignals()).contains(SEQUENCE_GAP);
        assertThat(late.qualitySignals()).contains(OUT_OF_ORDER);
        assertThat(jdbc.queryForObject("select highest_sequence from case_history_stream_cursor",
                Long.class)).isEqualTo(3L);
    }

    @Test
    void futureSourceTimeAndPeerDeclaredUsageAreLabelledRatherThanPromotedToTruth() {
        HistoryObservation base = observation("obs-19-peer", 1, SHA_A, null,
                List.of(link("remote-work-18-01", HistoryObservation.IdentityKind.REMOTE_WORK),
                        link("carrier-task-8842", PEER_TASK)));
        HistoryObservation peer = new HistoryObservation(base.tenantId(), base.observationId(),
                base.sourceStream(), base.sourceSequence(), base.messageId(), base.eventId(),
                base.payloadSha256(), "peer:carrier-cross-border", HistoryObservation.EventClass.REMOTE_WORK,
                "LATE", PEER_DECLARED, NOW.plusSeconds(600), base.identityLinks(), null,
                new HistoryObservation.UsageObservation(
                        HistoryObservation.UsageSource.PEER_DECLARED, 200, 3_000),
                new HistoryObservation.MeasuredUsage(250, 700), "REMOTE_RESULT_LATE",
                "remote-peer-metadata-v1", "OPERATIONS_90D", NOW.plusSeconds(86_400));

        HistoryReceipt receipt = record(peer);
        assertThat(receipt.qualitySignals()).contains(FUTURE_SOURCE_TIME, DECLARED_USAGE_ONLY);
        assertThat(jdbc.queryForObject("select usage_source from case_history_observation",
                String.class)).isEqualTo("PEER_DECLARED");
        assertThat(jdbc.queryForObject("select measured_duration_millis from case_history_observation",
                Long.class)).isEqualTo(250L);
    }

    @Test
    void reconciledAndLocallyCountedUsageAreNotLabelledAsDeclarationOnly() {
        HistoryObservation base = observation("obs-19-reconciled", 1, SHA_A, null, List.of());
        HistoryObservation reconciled = withUsage(base, HistoryObservation.UsageSource.BILLING_RECONCILED);
        assertThat(record(reconciled).qualitySignals()).doesNotContain(DECLARED_USAGE_ONLY);

        HistoryObservation localBase = observation("obs-19-local", 2, SHA_A, null, List.of());
        HistoryObservation local = withUsage(localBase, HistoryObservation.UsageSource.LOCAL_COUNTED);
        assertThat(record(local).qualitySignals()).doesNotContain(DECLARED_USAGE_ONLY);
    }

    @Test
    void viewsEnforceTenantPurposeAndExpiry() {
        record(observation("obs-19-scope", 1, SHA_A, null,
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE))));
        assertThatThrownBy(() -> history.view(scope(HistoryView.Purpose.AUDIT), TENANT, CASE,
                HistoryView.Purpose.EVALUATION)).isInstanceOf(SecurityException.class)
                .hasMessage("HISTORY_PURPOSE_DENIED");
        assertThatThrownBy(() -> history.view(scope(HistoryView.Purpose.AUDIT), "tenant-us",
                CASE, HistoryView.Purpose.AUDIT)).isInstanceOf(SecurityException.class)
                .hasMessage("HISTORY_TENANT_DENIED");
        clock.set(NOW.plusSeconds(3_601));
        assertThatThrownBy(() -> history.view(scopeAt(NOW.plusSeconds(3_600),
                HistoryView.Purpose.AUDIT), TENANT, CASE, HistoryView.Purpose.AUDIT))
                .isInstanceOf(SecurityException.class).hasMessage("HISTORY_SCOPE_EXPIRED");
    }

    @Test
    void metricDimensionsUseOnlyTheFixedLowCardinalityAllowlist() {
        assertThat(OpenTelemetryHistoryProjection.metricKeys())
                .containsExactlyInAnyOrder("stage", "outcome", "participant.kind", "quality")
                .doesNotContain("tenantId", "caseId", "runId", "effectId", "peerTaskId");
        assertThat(OpenTelemetryHistoryProjection.metricOutcomes())
                .containsExactlyInAnyOrder("success", "waiting", "partial", "denied",
                        "failed", "unknown", "late", "cancelled", "contained", "other");
        assertThat(OpenTelemetryHistoryProjection.boundedOutcome(CASE)).isEqualTo("other");
        assertThat(OpenTelemetryHistoryProjection.boundedOutcome("PROPOSAL_AVAILABLE"))
                .isEqualTo("success");
    }

    @Test
    void modelProvenanceRequiresExecutionIdentityAndAcceptedRunProposalDerivation() {
        HistoryObservation acceptedBase = observation("obs-19-accepted-proposal", 1, SHA_A, null,
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE),
                        link(RUN, HistoryObservation.IdentityKind.RUN),
                        link("proposal-019491", HistoryObservation.IdentityKind.PROPOSAL)));
        record(withEventClassAndOutcome(acceptedBase, BUSINESS_LIFECYCLE,
                "PROPOSAL_AVAILABLE"));

        HistoryObservation base = observation("obs-19-model-incomplete", 2, SHA_A, null,
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE),
                        link(RUN, HistoryObservation.IdentityKind.RUN),
                        link("proposal-019491", HistoryObservation.IdentityKind.PROPOSAL),
                        link("model:fixture-v1", HistoryObservation.IdentityKind.MODEL),
                        link("instruction:order-exception-v1",
                                HistoryObservation.IdentityKind.INSTRUCTION)));
        HistoryObservation incomplete = withEventClass(base, MODEL_INVOCATION);
        record(incomplete);
        assertThat(history.view(scope(HistoryView.Purpose.EVALUATION), TENANT, CASE,
                HistoryView.Purpose.EVALUATION).explicitGaps())
                .contains("MISSING_MODEL_EXECUTION_PROVENANCE");

        HistoryObservation unrelatedBase = observation("obs-19-model-unrelated", 3, SHA_B, null,
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE),
                        link("run-unrelated", HistoryObservation.IdentityKind.RUN),
                        link("proposal-unrelated", HistoryObservation.IdentityKind.PROPOSAL),
                        link("model:fixture-v1", HistoryObservation.IdentityKind.MODEL),
                        link("provider-execution:unrelated-123",
                                HistoryObservation.IdentityKind.MODEL_EXECUTION),
                        link("instruction:order-exception-v1",
                                HistoryObservation.IdentityKind.INSTRUCTION)));
        record(withEventClass(unrelatedBase, MODEL_INVOCATION));
        assertThat(history.view(scope(HistoryView.Purpose.EVALUATION), TENANT, CASE,
                HistoryView.Purpose.EVALUATION).explicitGaps())
                .contains("MISSING_MODEL_EXECUTION_PROVENANCE");

        HistoryObservation completeBase = observation("obs-19-model-complete", 4, SHA_B, null,
                List.of(link(CASE, HistoryObservation.IdentityKind.CASE),
                        link(RUN, HistoryObservation.IdentityKind.RUN),
                        link("proposal-019491", HistoryObservation.IdentityKind.PROPOSAL),
                        link("model:fixture-v1", HistoryObservation.IdentityKind.MODEL),
                        link("provider-execution:accepted-456",
                                HistoryObservation.IdentityKind.MODEL_EXECUTION),
                        link("instruction:order-exception-v1",
                                HistoryObservation.IdentityKind.INSTRUCTION)));
        record(withEventClass(completeBase, MODEL_INVOCATION));
        assertThat(history.view(scope(HistoryView.Purpose.EVALUATION), TENANT, CASE,
                HistoryView.Purpose.EVALUATION).explicitGaps())
                .doesNotContain("MISSING_MODEL_EXECUTION_PROVENANCE");
    }

    @Test
    // tag::authorized-replay-zero-effects-test[]
    void authorizedReconstructionCreatesNewIdentityAndHasZeroEffectSurface() {
        ReplayInputManifest manifest = storeManifest("manifest-19-01", "model:fixture-v1",
                "RETAINED");
        authorize("replay-authority-19-01", manifest, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        long attemptsBefore = count("effect_attempt");
        long outboxBefore = count("effect_outbox");

        ReplayReceipt receipt = replay(new ReplayCommand(TENANT, "replay-request-19-01",
                "replay-authority-19-01", manifest.manifestId()));

        assertThat(receipt.state()).isEqualTo("COMPLETED");
        assertThat(receipt.resultCode()).isEqualTo(ReplayEvaluator.ResultCode.RECONSTRUCTED);
        assertThat(receipt.explicitGaps()).contains(ReplayEvaluator.GapCode.FIXTURE_EVALUATOR);
        assertThat(receipt.replayRunId()).isNotEqualTo(receipt.sourceRunId());
        assertThat(evaluator.reconstructionCalls).isOne();
        assertThat(evaluator.reevaluationCalls).isZero();
        assertThat(count("effect_attempt")).isEqualTo(attemptsBefore);
        assertThat(count("effect_outbox")).isEqualTo(outboxBefore);
        assertThat(count("authorized_effect")).isZero();
    }
    // end::authorized-replay-zero-effects-test[]

    @Test
    void replayRejectsExpiredAuthorityTenantMismatchAndAlteredRetainedInput() {
        ReplayInputManifest manifest = storeManifest("manifest-19-expired", "model:fixture-v1",
                "RETAINED");
        authorize("authority-expired", manifest, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.minusSeconds(1));
        assertThatThrownBy(() -> replay(command("replay-expired", "authority-expired", manifest)))
                .hasRootCauseMessage("REPLAY_AUTHORITY_EXPIRED");

        authorize("authority-active", manifest, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        jdbc.update("update replay_input_manifest set snapshot_sha256=? where tenant_id=?",
                SHA_B, TENANT);
        assertThatThrownBy(() -> replay(command("replay-altered", "authority-active", manifest)))
                .hasRootCauseMessage("RETAINED_INPUT_CHANGED");
        assertThat(count("replay_execution")).isZero();

        assertThatThrownBy(() -> replay(new ReplayCommand("tenant-us", "replay-cross-tenant",
                "authority-active", manifest.manifestId())))
                .hasRootCauseMessage("REPLAY_TENANT_DENIED");
    }

    @Test
    void replayDuplicateIsStableCollisionIsRejectedAndMissingModelIsNotSubstituted() {
        ReplayInputManifest manifest = storeManifest("manifest-19-dup", "model:fixture-v1",
                "RETAINED");
        authorize("authority-dup", manifest, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        ReplayCommand command = command("replay-19-dup", "authority-dup", manifest);
        assertThat(replay(command).state()).isEqualTo("COMPLETED");
        assertThat(replay(command).state()).isEqualTo("COMPLETED");
        assertThat(evaluator.reconstructionCalls).isOne();
        jdbc.update("""
                update replay_authorization set authority_state='REVOKED'
                where tenant_id=? and authorization_id=?
                """, TENANT, "authority-dup");
        assertThatThrownBy(() -> replay(command))
                .hasRootCauseMessage("REPLAY_AUTHORITY_NOT_ACTIVE");
        jdbc.update("""
                update replay_authorization set authority_state='ACTIVE'
                where tenant_id=? and authorization_id=?
                """, TENANT, "authority-dup");
        reset();
        ReplayInputManifest missing = storeManifest("manifest-19-missing-model", null, "RETAINED");
        authorize("authority-reevaluate", missing, "REEVALUATE", "MODEL_EVALUATION",
                "ACTIVE", NOW.plusSeconds(600));
        assertThatThrownBy(() -> replay(command("replay-missing-model",
                "authority-reevaluate", missing)))
                .hasRootCauseMessage("PINNED_IMPLEMENTATION_UNAVAILABLE");
        assertThat(count("replay_execution")).isZero();
    }

    @Test
    void replayFingerprintIsDerivedAndChangedFieldsCollideWithoutACallerDigest() {
        ReplayInputManifest first = storeManifest("manifest-19-fingerprint-a", "model:fixture-v1",
                "RETAINED");
        authorize("authority-fingerprint-a", first, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        ReplayCommand original = command("replay-19-fingerprint", "authority-fingerprint-a", first);
        assertThat(replay(original).state()).isEqualTo("COMPLETED");
        assertThat(jdbc.queryForObject("select request_sha256 from replay_execution",
                String.class)).isEqualTo(original.calculatedSha256());

        ReplayInputManifest second = storeManifest("manifest-19-fingerprint-b", "model:fixture-v1",
                "RETAINED");
        authorize("authority-fingerprint-b", second, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        ReplayCommand changed = new ReplayCommand(TENANT, original.replayId(),
                "authority-fingerprint-b", second.manifestId());
        assertThatThrownBy(() -> replay(changed))
                .hasRootCauseMessage("REPLAY_ID_COLLISION");
    }

    @Test
    void oneUseAuthorityAllowsCurrentDuplicateButNoDistinctCostlyReplay() {
        ReplayInputManifest manifest = storeManifest("manifest-19-single-use", "model:fixture-v1",
                "RETAINED");
        authorize("authority-single-use", manifest, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        ReplayCommand first = command("replay-19-single-use-a", "authority-single-use", manifest);
        assertThat(replay(first).state()).isEqualTo("COMPLETED");
        assertThat(replay(first).state()).isEqualTo("COMPLETED");
        assertThat(evaluator.reconstructionCalls).isOne();
        assertThat(jdbc.queryForObject("""
                select uses_consumed from replay_authorization
                where tenant_id=? and authorization_id=?
                """, Integer.class, TENANT, "authority-single-use")).isOne();

        assertThatThrownBy(() -> replay(command("replay-19-single-use-b",
                "authority-single-use", manifest)))
                .hasRootCauseMessage("REPLAY_AUTHORITY_USE_EXHAUSTED");
        assertThat(evaluator.reconstructionCalls).isOne();
    }

    @Test
    void evaluatorFailureAfterClaimLeavesDurableUnknownOutcomeAndCannotBeRetried() {
        ReplayInputManifest manifest = storeManifest("manifest-19-evaluator-failure",
                "model:fixture-v1", "RETAINED");
        authorize("authority-evaluator-failure", manifest, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        ReplayCommand command = command("replay-19-evaluator-failure",
                "authority-evaluator-failure", manifest);
        long attemptsBefore = count("effect_attempt");
        long outboxBefore = count("effect_outbox");
        evaluator.failNextReconstruction();

        ReplayReceipt failed = replay(command);

        assertThat(failed.state()).isEqualTo("OUTCOME_UNKNOWN");
        assertThat(failed.resultCode()).isNull();
        assertThat(failed.explicitGaps())
                .containsExactly(ReplayEvaluator.GapCode.EVALUATION_OUTCOME_UNKNOWN);
        assertThat(evaluator.reconstructionCalls).isOne();
        assertThat(evaluator.transactionActiveDuringEvaluation).isFalse();
        assertThat(jdbc.queryForObject("""
                select execution_state from replay_execution
                where tenant_id=? and replay_id=?
                """, String.class, TENANT, command.replayId())).isEqualTo("OUTCOME_UNKNOWN");
        assertThat(jdbc.queryForObject("""
                select uses_consumed from replay_authorization
                where tenant_id=? and authorization_id=?
                """, Integer.class, TENANT, command.authorizationId())).isOne();

        assertThat(replay(command).state()).isEqualTo("OUTCOME_UNKNOWN");
        assertThat(evaluator.reconstructionCalls).isOne();
        assertThatThrownBy(() -> replay(command("replay-19-evaluator-failure-distinct",
                command.authorizationId(), manifest)))
                .hasRootCauseMessage("REPLAY_AUTHORITY_USE_EXHAUSTED");
        assertThat(evaluator.reconstructionCalls).isOne();
        assertThat(count("effect_attempt")).isEqualTo(attemptsBefore);
        assertThat(count("effect_outbox")).isEqualTo(outboxBefore);
        assertThat(count("authorized_effect")).isZero();
    }

    @Test
    void duplicateOfCommittedRunningClaimDoesNotInvokeEvaluatorAgain() {
        ReplayInputManifest manifest = storeManifest("manifest-19-running",
                "model:fixture-v1", "RETAINED");
        authorize("authority-running", manifest, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        ReplayCommand command = command("replay-19-running", "authority-running", manifest);
        jdbc.update("""
                update replay_authorization set uses_consumed=1
                where tenant_id=? and authorization_id=?
                """, TENANT, command.authorizationId());
        jdbc.update("""
                insert into replay_execution
                (tenant_id, replay_id, replay_run_id, source_run_id, authorization_id,
                 manifest_id, manifest_sha256, replay_mode, purpose, execution_state,
                 request_sha256, started_at)
                values (?,?,?,?,?,?,?,?,?,'RUNNING',?,?)
                """, TENANT, command.replayId(), "replay-run:" + command.replayId(), RUN,
                command.authorizationId(), manifest.manifestId(), manifest.manifestSha256(),
                "RECONSTRUCT", "INCIDENT_REVIEW", command.calculatedSha256(), NOW);

        ReplayReceipt duplicate = replay(command);

        assertThat(duplicate.state()).isEqualTo("RUNNING");
        assertThat(evaluator.reconstructionCalls).isZero();
        assertThat(jdbc.queryForObject("""
                select uses_consumed from replay_authorization
                where tenant_id=? and authorization_id=?
                """, Integer.class, TENANT, command.authorizationId())).isOne();
    }

    @Test
    void replayAuthorityIdIsNotPermissionWithoutMatchingProtectedActorAndPurpose() {
        ReplayInputManifest manifest = storeManifest("manifest-19-actor", "model:fixture-v1",
                "RETAINED");
        authorize("authority-actor", manifest, "RECONSTRUCT", "INCIDENT_REVIEW",
                "ACTIVE", NOW.plusSeconds(600));
        ReplayCommand command = command("replay-19-actor", "authority-actor", manifest);

        replayScopes.useForTest(new AuthorizedReplayScope(
                "actor:someone-else", Set.of(TENANT), Set.of("INCIDENT_REVIEW"),
                JdbcReplayManager.AUDIENCE, JdbcReplayManager.SERVICE, NOW.plusSeconds(600)));
        assertThatThrownBy(() -> replay(command)).hasRootCauseMessage("REPLAY_ACTOR_MISMATCH");
        assertThat(count("replay_execution")).isZero();

        replayScopes.useForTest(new AuthorizedReplayScope(
                "actor:recovery-owner-19", Set.of(TENANT), Set.of("MODEL_EVALUATION"),
                JdbcReplayManager.AUDIENCE, JdbcReplayManager.SERVICE, NOW.plusSeconds(600)));
        assertThatThrownBy(() -> replay(command)).hasRootCauseMessage("REPLAY_PURPOSE_DENIED");
        assertThat(count("replay_execution")).isZero();
    }

    @Test
    void manifestCanonicalizationDistinguishesEmbeddedSeparators() {
        ReplayInputManifest first = manifestDraft("manifest-19-canonical", "model:a\nb", "c");
        ReplayInputManifest second = manifestDraft("manifest-19-canonical", "model:a", "b\nc");
        assertThat(first.calculatedSha256()).isNotEqualTo(second.calculatedSha256());
    }

    @Test
    void namedButUnavailablePinnedModelIsNotSilentlySubstituted() {
        ReplayInputManifest unavailable = storeManifest("manifest-19-unknown-model",
                "model:named-but-unavailable", "RETAINED");
        authorize("authority-unknown-model", unavailable, "REEVALUATE", "MODEL_EVALUATION",
                "ACTIVE", NOW.plusSeconds(600));
        assertThatThrownBy(() -> replay(command("replay-unknown-model",
                "authority-unknown-model", unavailable)))
                .hasRootCauseMessage("PINNED_IMPLEMENTATION_UNAVAILABLE");
        assertThat(count("replay_execution")).isZero();
    }

    private HistoryReceipt record(HistoryObservation observation) {
        return producer.requestBody("direct:record-history-observation", observation,
                HistoryReceipt.class);
    }

    private ReplayReceipt replay(ReplayCommand command) {
        return producer.requestBody("direct:execute-authorized-replay", command,
                ReplayReceipt.class);
    }

    private HistoryObservation observation(String id, long sequence, String sha,
            HistoryObservation.TraceContext trace, List<HistoryObservation.IdentityLink> links) {
        return new HistoryObservation(TENANT, id, "resolution-plan-019494", sequence,
                "msg:" + id, "evt:" + id, sha, "service:order-desk", BUSINESS_LIFECYCLE,
                "RECORDED", APPLICATION_RECORDED, NOW.minusSeconds(20), links, trace,
                null, new HistoryObservation.MeasuredUsage(15, 200), "DURABLE_FACT_RECORDED",
                "business-metadata-v1", "AUDIT_7Y", NOW.plusSeconds(86_400));
    }

    private HistoryObservation withUsage(
            HistoryObservation base, HistoryObservation.UsageSource source) {
        return new HistoryObservation(base.tenantId(), base.observationId(), base.sourceStream(),
                base.sourceSequence(), base.messageId(), base.eventId(), base.payloadSha256(),
                base.sourceOwner(), base.eventClass(), base.outcomeCode(), base.trustClass(),
                base.occurredAt(), base.identityLinks(), base.traceContext(),
                new HistoryObservation.UsageObservation(source, 100, 2_000),
                base.measuredUsage(), base.summaryCode(), base.redactionProfile(),
                base.retentionClass(), base.retainUntil());
    }

    private HistoryObservation withEventClass(
            HistoryObservation base, HistoryObservation.EventClass eventClass) {
        return withEventClassAndOutcome(base, eventClass, base.outcomeCode());
    }

    private HistoryObservation withEventClassAndOutcome(
            HistoryObservation base, HistoryObservation.EventClass eventClass,
            String outcomeCode) {
        return new HistoryObservation(base.tenantId(), base.observationId(), base.sourceStream(),
                base.sourceSequence(), base.messageId(), base.eventId(), base.payloadSha256(),
                base.sourceOwner(), eventClass, outcomeCode, base.trustClass(),
                base.occurredAt(), base.identityLinks(), base.traceContext(),
                base.usageObservation(), base.measuredUsage(), base.summaryCode(),
                base.redactionProfile(), base.retentionClass(), base.retainUntil());
    }

    private HistoryObservation.IdentityLink link(
            String value, HistoryObservation.IdentityKind kind) {
        return new HistoryObservation.IdentityLink(kind, value);
    }

    private AuthorizedHistoryScope scope(HistoryView.Purpose purpose) {
        return scopeAt(NOW.plusSeconds(3_600), purpose);
    }

    private AuthorizedHistoryScope scopeAt(Instant expires, HistoryView.Purpose purpose) {
        return new AuthorizedHistoryScope("actor:operator-19", Set.of(TENANT),
                Set.of(purpose), expires);
    }

    private ReplayInputManifest storeManifest(String id, String modelRef, String state) {
        ReplayInputManifest draft = manifestDraft(id, modelRef,
                "order-exception-investigation-v1", state);
        ReplayInputManifest manifest = new ReplayInputManifest(draft.tenantId(), draft.manifestId(),
                draft.sourceCaseId(), draft.sourceRunId(), draft.asOfEventId(), draft.snapshotRef(),
                draft.snapshotSha256(), draft.evidenceSetRef(), draft.evidenceSetSha256(),
                draft.modelRef(), draft.instructionRef(), draft.toolCatalogRef(), draft.policyRef(),
                draft.configurationRef(), draft.calculatedSha256(), draft.retentionState());
        jdbc.update("""
                insert into replay_input_manifest
                (tenant_id, manifest_id, source_case_id, source_run_id, as_of_event_id,
                 snapshot_ref, snapshot_sha256, evidence_set_ref, evidence_set_sha256,
                 model_ref, instruction_ref, tool_catalog_ref, policy_ref, configuration_ref,
                 manifest_sha256, retention_state, created_at)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, manifest.tenantId(), manifest.manifestId(), manifest.sourceCaseId(),
                manifest.sourceRunId(), manifest.asOfEventId(), manifest.snapshotRef(),
                manifest.snapshotSha256(), manifest.evidenceSetRef(), manifest.evidenceSetSha256(),
                manifest.modelRef(), manifest.instructionRef(), manifest.toolCatalogRef(),
                manifest.policyRef(), manifest.configurationRef(), manifest.manifestSha256(),
                manifest.retentionState(), NOW);
        return manifest;
    }

    private ReplayInputManifest manifestDraft(String id, String modelRef, String instructionRef) {
        return manifestDraft(id, modelRef, instructionRef, "RETAINED");
    }

    private ReplayInputManifest manifestDraft(
            String id, String modelRef, String instructionRef, String state) {
        return new ReplayInputManifest(TENANT, id, CASE, RUN,
                "evt-019500", "snapshot-8c65c449-da5e-3075-a0b6-fd444f8bd1f0", SHA_A,
                "artifact://tenant-ca/evidence/set-018", SHA_B, modelRef,
                instructionRef, "order-desk-capabilities-v1",
                "policy://tenant-ca/order-effects/v3", "order-exception-ca-17",
                SHA_A, state);
    }

    private void authorize(String id, ReplayInputManifest manifest, String mode, String purpose,
            String state, Instant expires) {
        jdbc.update("""
                insert into replay_authorization
                (tenant_id, authorization_id, manifest_id, manifest_sha256, replay_mode,
                 purpose, actor_ref, policy_ref, issued_at, expires_at, authority_state,
                 max_uses, uses_consumed, effect_mode)
                values (?,?,?,?,?,?,?,?,?,?,?,1,0,'FORBIDDEN')
                """, TENANT, id, manifest.manifestId(), manifest.manifestSha256(), mode,
                purpose, "actor:recovery-owner-19", "policy://tenant-ca/replay/v1",
                NOW.minusSeconds(60), expires, state);
    }

    private ReplayCommand command(String replayId, String authorityId,
            ReplayInputManifest manifest) {
        return new ReplayCommand(TENANT, replayId, authorityId, manifest.manifestId());
    }

    private long count(String table) {
        return jdbc.queryForObject("select count(*) from " + table, Long.class);
    }

    @TestConfiguration
    static class TestBeans {
        @Bean @Primary TestClock historyClock() { return new TestClock(NOW); }
        @Bean @Primary CountingReplayEvaluator countingReplayEvaluator() {
            return new CountingReplayEvaluator();
        }
    }

    static class TestClock extends Clock {
        private Instant instant;
        TestClock(Instant instant) { this.instant = instant; }
        void set(Instant instant) { this.instant = instant; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return instant; }
    }

    static class CountingReplayEvaluator implements ReplayEvaluator {
        int reconstructionCalls;
        int reevaluationCalls;
        boolean failNextReconstruction;
        boolean transactionActiveDuringEvaluation;
        void reset() {
            reconstructionCalls = 0;
            reevaluationCalls = 0;
            failNextReconstruction = false;
            transactionActiveDuringEvaluation = false;
        }
        void failNextReconstruction() { failNextReconstruction = true; }
        @Override public boolean isAvailable(String mode, ReplayInputManifest manifest) {
            return "RECONSTRUCT".equals(mode)
                    || ("REEVALUATE".equals(mode)
                        && Objects.equals("model:fixture-v1", manifest.modelRef()));
        }
        @Override public Result reconstruct(ReplayInputManifest manifest) {
            reconstructionCalls++;
            transactionActiveDuringEvaluation =
                    TransactionSynchronizationManager.isActualTransactionActive();
            if (failNextReconstruction) {
                failNextReconstruction = false;
                throw new IllegalStateException("FORCED_EVALUATOR_FAILURE");
            }
            return new Result(ResultCode.RECONSTRUCTED,
                    ReplayInputManifest.sha256("reconstruct\n" + manifest.manifestSha256()),
                    Set.of(GapCode.FIXTURE_EVALUATOR));
        }
        @Override public Result reevaluate(ReplayInputManifest manifest) {
            reevaluationCalls++;
            transactionActiveDuringEvaluation =
                    TransactionSynchronizationManager.isActualTransactionActive();
            if (!Objects.equals("model:fixture-v1", manifest.modelRef()))
                throw new IllegalStateException("PINNED_IMPLEMENTATION_UNAVAILABLE");
            return new Result(ResultCode.REEVALUATED,
                    ReplayInputManifest.sha256("reevaluate\n" + manifest.manifestSha256()),
                    Set.of(GapCode.FIXTURE_EVALUATOR));
        }
    }
}
