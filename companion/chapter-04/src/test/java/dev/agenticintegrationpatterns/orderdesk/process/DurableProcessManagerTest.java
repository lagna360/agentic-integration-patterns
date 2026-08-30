package dev.agenticintegrationpatterns.orderdesk.process;

import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static dev.agenticintegrationpatterns.orderdesk.process.EvidenceSetClosed.Decision.EVIDENCE_READY;
import static dev.agenticintegrationpatterns.orderdesk.process.EvidenceSetClosed.Decision.REVIEW_REQUIRED;
import static dev.agenticintegrationpatterns.orderdesk.process.EvidenceSetClosed.Reason.EVIDENCE_CONFLICT;
import static dev.agenticintegrationpatterns.orderdesk.process.EvidenceSetClosed.Reason.OPTIONAL_EVIDENCE_MISSING;
import static dev.agenticintegrationpatterns.orderdesk.process.ProcessReceipt.Disposition.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(DurableProcessManagerTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class DurableProcessManagerTest {
    private static final Instant NOW = Instant.parse("2026-08-25T14:00:00Z");

    @Autowired JdbcDurableProcessManager manager;
    @Autowired JdbcTemplate jdbc;
    @Autowired ProducerTemplate producer;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired FailOnceAfterState hook;
    @Autowired AdjustableClock clock;

    @BeforeEach
    void reset() {
        jdbc.update("delete from process_outbox");
        jdbc.update("delete from process_message_rejection");
        jdbc.update("delete from process_message_inbox");
        jdbc.update("delete from investigation_expected_work");
        jdbc.update("delete from investigation_run");
        hook.reset();
        clock.set(NOW);
    }

    @Test
    // tag::durable-process-route-test[]
    void camelHandsTypedWorkToATransactionalProcessManager() {
        var receipt = producer.requestBody(
                "direct:start-investigation-run", start("start-1"), ProcessReceipt.class);

        assertThat(receipt.disposition()).isEqualTo(APPLIED);
        assertThat(receipt.state()).isEqualTo(RunState.WAITING_FOR_EVIDENCE);
        assertThat(jdbc.queryForObject(
                "select count(*) from investigation_expected_work where tenant_id=? and run_id=?",
                Integer.class, "tenant-ca", "run-11")).isEqualTo(2);
        assertThat(jdbc.queryForObject(
                "select count(*) from process_outbox where event_type='InvestigationRunStarted'",
                Integer.class)).isEqualTo(1);
    }
    // end::durable-process-route-test[]

    @Test
    void identicalInputIsDuplicateButChangedContentIsARecordedCollision() {
        assertThat(manager.start(start("start-1")).disposition()).isEqualTo(APPLIED);
        assertThat(manager.start(start("start-1")).disposition()).isEqualTo(DUPLICATE_SAME);

        var changed = new StartInvestigationRun(
                "start-1", "tenant-ca", "run-11", "case-B", "corr-11", "cmd-11",
                "parallel-plan-v1", NOW.plusSeconds(60), Set.of("inventory", "orders"),
                Set.of("inventory"), NOW);
        assertThat(manager.start(changed).disposition()).isEqualTo(MESSAGE_ID_COLLISION);
        assertThat(jdbc.queryForObject(
                "select count(*) from process_message_rejection", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from investigation_run", Integer.class)).isEqualTo(1);
    }

    @Test
    void aNewStartMessageCannotRestartAnExistingRun() {
        manager.start(start("start-1"));

        assertThat(manager.start(start("start-2")).disposition()).isEqualTo(OUT_OF_ORDER);
        assertThat(jdbc.queryForObject("""
                select count(*) from process_outbox where event_type='InvestigationRunStarted'
                """, Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("""
                select disposition from process_message_inbox where message_id='start-2'
                """, String.class)).isEqualTo("OUT_OF_ORDER");
    }

    @Test
    // tag::fencing-recovery-test[]
    void expiredLeaseIsReclaimedAndTheStaleWorkerCannotCommit() {
        manager.start(start("start-1"));
        var oldLease = manager.claimNextDue("worker-A", Duration.ofSeconds(5)).orElseThrow();
        clock.set(NOW.plusSeconds(6));
        var newLease = manager.claimNextDue("worker-B", Duration.ofSeconds(20)).orElseThrow();

        assertThat(newLease.fenceToken()).isGreaterThan(oldLease.fenceToken());
        clock.set(NOW.plusSeconds(7));
        assertThat(manager.applyEvidence(oldLease, evidence("evidence-old", NOW.plusSeconds(7)))
                .disposition()).isEqualTo(STALE_FENCE);

        var applied = manager.applyEvidence(
                newLease, evidence("evidence-new", NOW.plusSeconds(8)));
        assertThat(applied.disposition()).isEqualTo(APPLIED);
        assertThat(applied.state()).isEqualTo(RunState.READY_FOR_ASSESSMENT);
        assertThat(jdbc.queryForObject(
                "select attempt_count from investigation_run where tenant_id=? and run_id=?",
                Integer.class, "tenant-ca", "run-11")).isEqualTo(2);
    }
    // end::fencing-recovery-test[]

    @Test
    void expiredLeaseCannotWriteEvenBeforeAnotherWorkerClaimsIt() {
        manager.start(start("start-1"));
        var expired = manager.claimNextDue("worker-A", Duration.ofSeconds(5)).orElseThrow();

        clock.set(NOW.plusSeconds(7));
        assertThat(manager.applyEvidence(
                expired, evidence("evidence-expired", NOW.plusSeconds(4))).disposition())
                .isEqualTo(STALE_FENCE);
        assertThat(jdbc.queryForObject("""
                select state from investigation_run where tenant_id='tenant-ca' and run_id='run-11'
                """, String.class)).isEqualTo("WAITING_FOR_EVIDENCE");
    }

    @Test
    void stateExpectedWorkInboxAndOutboxCommitTogether() {
        manager.start(start("start-1"));
        var lease = manager.claimNextDue("worker-A", Duration.ofSeconds(20)).orElseThrow();
        clock.set(NOW.plusSeconds(2));

        var receipt = producer.requestBody("direct:accept-claimed-evidence",
                new LeasedEvidenceClosure(lease, evidence("evidence-1", NOW.plusSeconds(2))),
                ProcessReceipt.class);

        assertThat(receipt.state()).isEqualTo(RunState.READY_FOR_ASSESSMENT);
        assertThat(jdbc.queryForList("""
                select work_status from investigation_expected_work
                 where tenant_id='tenant-ca' and run_id='run-11' order by work_name
                """, String.class)).containsExactly("SUCCEEDED", "UNAVAILABLE");
        assertThat(jdbc.queryForObject("""
                select count(*) from process_outbox
                 where event_type='ParallelEvidenceAccepted' and aggregate_version=1
                """, Integer.class)).isEqualTo(1);
    }

    @Test
    // tag::forced-rollback-test[]
    void crashBetweenStateMutationAndOutboxInsertRollsBackTheWholeTransition() {
        manager.start(start("start-1"));
        var lease = manager.claimNextDue("worker-A", Duration.ofSeconds(20)).orElseThrow();
        hook.failNext();
        clock.set(NOW.plusSeconds(2));

        assertThatThrownBy(() -> manager.applyEvidence(
                lease, evidence("evidence-1", NOW.plusSeconds(2))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("forced crash");

        assertThat(jdbc.queryForObject("""
                select state from investigation_run where tenant_id='tenant-ca' and run_id='run-11'
                """, String.class)).isEqualTo("WAITING_FOR_EVIDENCE");
        assertThat(jdbc.queryForList("""
                select work_status from investigation_expected_work
                 where tenant_id='tenant-ca' and run_id='run-11' order by work_name
                """, String.class)).containsExactly("EXPECTED", "EXPECTED");
        assertThat(jdbc.queryForObject("""
                select count(*) from process_message_inbox where message_id='evidence-1'
                """, Integer.class)).isZero();
        assertThat(jdbc.queryForObject("""
                select count(*) from process_outbox where event_type='ParallelEvidenceAccepted'
                """, Integer.class)).isZero();
    }
    // end::forced-rollback-test[]

    @Test
    void persistedDeadlineSurvivesLeaseExpiryAndStopsOnce() {
        manager.start(start("start-1"));
        var abandoned = manager.claimNextDue("dead-worker", Duration.ofSeconds(5)).orElseThrow();
        clock.set(NOW.plusSeconds(61));
        var recovered = manager.claimNextDue("recovery-worker", Duration.ofSeconds(20)).orElseThrow();

        assertThat(manager.stopAtDeadline("deadline-1", abandoned).disposition())
                .isEqualTo(STALE_FENCE);
        clock.set(NOW.plusSeconds(62));
        var stopped = manager.stopAtDeadline("deadline-2", recovered);
        assertThat(stopped.state()).isEqualTo(RunState.STOPPED);
        clock.set(NOW.plusSeconds(63));
        assertThat(manager.stopAtDeadline("deadline-2", recovered)
                .disposition()).isEqualTo(DUPLICATE_SAME);
    }

    @Test
    void overdueRunCanBeClaimedOnlyForDeadlineWorkAndCannotAcceptEvidence() {
        manager.start(start("start-1"));
        clock.set(NOW.plusSeconds(61));
        var deadlineLease = manager.claimNextDue(
                "deadline-worker", Duration.ofSeconds(20)).orElseThrow();

        assertThat(deadlineLease.purpose()).isEqualTo(RunLease.Purpose.DEADLINE);
        assertThat(manager.applyEvidence(
                deadlineLease, evidence("evidence-late", NOW.plusSeconds(59))).disposition())
                .isEqualTo(DEADLINE_EXCEEDED);
        assertThat(jdbc.queryForObject("""
                select state from investigation_run where tenant_id='tenant-ca' and run_id='run-11'
                """, String.class)).isEqualTo("WAITING_FOR_EVIDENCE");
        assertThat(jdbc.queryForObject("""
                select disposition from process_message_inbox where message_id='evidence-late'
                """, String.class)).isEqualTo("DEADLINE_EXCEEDED");
    }

    @Test
    void pausePreservesTheAbsoluteDeadlineAndTheDeadlineScannerStillStopsIt() {
        manager.start(start("start-1"));
        clock.set(NOW.plusSeconds(5));
        assertThat(manager.pause("pause-1", "tenant-ca", "run-11").state())
                .isEqualTo(RunState.PAUSED);
        clock.set(NOW.plusSeconds(20));
        assertThat(manager.resume("resume-1", "tenant-ca", "run-11").state())
                .isEqualTo(RunState.WAITING_FOR_EVIDENCE);
        assertThat(jdbc.queryForObject("""
                select deadline_at from investigation_run
                 where tenant_id='tenant-ca' and run_id='run-11'
                """, Instant.class)).isEqualTo(NOW.plusSeconds(60));

        clock.set(NOW.plusSeconds(30));
        manager.pause("pause-2", "tenant-ca", "run-11");
        clock.set(NOW.plusSeconds(61));
        var timerLease = manager.claimNextDue("timer-worker", Duration.ofSeconds(20)).orElseThrow();
        clock.set(NOW.plusSeconds(62));
        assertThat(manager.stopAtDeadline("deadline-paused", timerLease).state())
                .isEqualTo(RunState.STOPPED);
    }

    @Test
    void resumeAfterTheAbsoluteDeadlineBecomesAStopDecision() {
        manager.start(start("start-1"));
        clock.set(NOW.plusSeconds(5));
        manager.pause("pause-1", "tenant-ca", "run-11");
        clock.set(NOW.plusSeconds(70));

        var receipt = manager.resume("resume-late", "tenant-ca", "run-11");

        assertThat(receipt.state()).isEqualTo(RunState.STOPPED);
        assertThat(jdbc.queryForObject("""
                select updated_at from investigation_run
                 where tenant_id='tenant-ca' and run_id='run-11'
                """, Instant.class)).isEqualTo(NOW.plusSeconds(70));
        assertThat(jdbc.queryForObject("""
                select count(*) from process_outbox
                 where event_type='InvestigationRunStopped' and created_at=?
                """, Integer.class, NOW.plusSeconds(70))).isEqualTo(1);
        assertThat(jdbc.queryForObject("""
                select event_payload from process_outbox
                 where event_type='InvestigationRunStopped' and created_at=?
                """, String.class, NOW.plusSeconds(70)))
                .contains("\"state\":\"STOPPED\"")
                .contains("\"reason\":\"DEADLINE_EXCEEDED\"")
                .contains("\"deadlineAt\":\"2026-08-25T14:01:00Z\"");
    }

    @Test
    // tag::outbox-crash-window-test[]
    void crashAfterPublishCausesAnHonestDuplicateWithTheSameEventId() {
        manager.start(start("start-1"));
        var published = new ArrayList<ProcessOutboxEvent>();
        var crash = new AtomicBoolean(true);
        var relay = new ProcessOutboxRelay(jdbc, new TransactionTemplate(transactionManager),
                published::add, () -> {
                    if (crash.getAndSet(false)) {
                        throw new IllegalStateException("forced crash after publish");
                    }
                }, Clock.fixed(NOW.plusSeconds(10), ZoneOffset.UTC));

        assertThatThrownBy(relay::relayOne).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject(
                "select count(*) from process_outbox where published_at is null",
                Integer.class)).isEqualTo(1);
        relay.relayOne();

        assertThat(published).hasSize(2);
        assertThat(published.get(0).eventId()).isEqualTo(published.get(1).eventId());
        assertThat(jdbc.queryForObject(
                "select count(*) from process_outbox where published_at is null",
                Integer.class)).isZero();
    }
    // end::outbox-crash-window-test[]

    @Test
    void mismatchedExpectedWorkIsDurablyRejectedAndRedeliveryIsADuplicate() {
        manager.start(start("start-1"));
        var lease = manager.claimNextDue("worker-A", Duration.ofSeconds(20)).orElseThrow();
        var incomplete = new EvidenceSetClosed(
                "evidence-1", "tenant-ca", "run-11", "artifact://tenant-ca/evidence/set-11",
                "a".repeat(64), EVIDENCE_READY, OPTIONAL_EVIDENCE_MISSING,
                Set.of("inventory"), Set.of(), Set.of(), NOW.plusSeconds(2));

        assertThat(manager.applyEvidence(lease, incomplete).disposition())
                .isEqualTo(INVALID_EVIDENCE);
        assertThat(manager.applyEvidence(lease, incomplete).disposition())
                .isEqualTo(DUPLICATE_SAME);
        assertThat(jdbc.queryForObject("""
                select disposition from process_message_inbox where message_id='evidence-1'
                """, String.class)).isEqualTo("INVALID_EVIDENCE");
        assertThat(jdbc.queryForObject("""
                select reason from process_message_rejection where message_id='evidence-1'
                """, String.class)).isEqualTo("EXPECTED_WORK_MISMATCH");
        assertThat(jdbc.queryForList("""
                select work_status from investigation_expected_work
                 where tenant_id='tenant-ca' and run_id='run-11' order by work_name
                """, String.class)).containsExactly("EXPECTED", "EXPECTED");
    }

    @Test
    void producerCannotCallMissingRequiredEvidenceReady() {
        manager.start(start("start-1"));
        var lease = manager.claimNextDue("worker-A", Duration.ofSeconds(20)).orElseThrow();
        var falseReady = new EvidenceSetClosed(
                "evidence-false-ready", "tenant-ca", "run-11",
                "artifact://tenant-ca/evidence/set-11", "a".repeat(64),
                EVIDENCE_READY, OPTIONAL_EVIDENCE_MISSING,
                Set.of("orders"), Set.of(), Set.of("inventory"), NOW.plusSeconds(2));

        assertThat(manager.applyEvidence(lease, falseReady).disposition())
                .isEqualTo(INVALID_EVIDENCE);
        assertThat(jdbc.queryForObject("""
                select state from investigation_run where tenant_id='tenant-ca' and run_id='run-11'
                """, String.class)).isEqualTo("WAITING_FOR_EVIDENCE");
        assertThat(jdbc.queryForObject("""
                select reason from process_message_rejection
                 where message_id='evidence-false-ready'
                """, String.class)).isEqualTo("REQUIRED_WORK_POLICY_CONTRADICTION");
    }

    @Test
    void persistedConflictReasonMapsTheChapter10ReviewDispositionToReviewState() {
        manager.start(start("start-1"));
        var lease = manager.claimNextDue("worker-A", Duration.ofSeconds(20)).orElseThrow();
        clock.set(NOW.plusSeconds(2));
        var conflict = new EvidenceSetClosed(
                "evidence-conflict", "tenant-ca", "run-11",
                "artifact://tenant-ca/evidence/set-conflict", "b".repeat(64),
                REVIEW_REQUIRED, EVIDENCE_CONFLICT,
                Set.of("inventory", "orders"), Set.of(), Set.of(), NOW.plusSeconds(2));

        var receipt = manager.applyEvidence(lease, conflict);

        assertThat(receipt.state()).isEqualTo(RunState.REVIEW_REQUIRED);
        assertThat(jdbc.queryForObject("""
                select completion_decision from investigation_run
                 where tenant_id='tenant-ca' and run_id='run-11'
                """, String.class)).isEqualTo("REVIEW_REQUIRED");
    }

    @Test
    void versionChangeIsRejectedSeparatelyFromLeaseFencing() {
        manager.start(start("start-1"));
        var lease = manager.claimNextDue("worker-A", Duration.ofSeconds(20)).orElseThrow();
        jdbc.update("""
                update investigation_run set version=version+1
                 where tenant_id='tenant-ca' and run_id='run-11'
                """);
        clock.set(NOW.plusSeconds(2));

        assertThatThrownBy(() -> manager.applyEvidence(
                lease, evidence("evidence-version", NOW.plusSeconds(2))))
                .isInstanceOf(JdbcDurableProcessManager.ConcurrentRunUpdateException.class)
                .hasMessageContaining("version changed");
        assertThat(jdbc.queryForObject("""
                select state from investigation_run where tenant_id='tenant-ca' and run_id='run-11'
                """, String.class)).isEqualTo("WAITING_FOR_EVIDENCE");
    }

    private StartInvestigationRun start(String messageId) {
        return new StartInvestigationRun(
                messageId, "tenant-ca", "run-11", "case-A", "corr-11", "cmd-11",
                "parallel-plan-v1", NOW.plusSeconds(60), Set.of("inventory", "orders"),
                Set.of("inventory"), NOW);
    }

    private EvidenceSetClosed evidence(String messageId, Instant receivedAt) {
        return new EvidenceSetClosed(
                messageId, "tenant-ca", "run-11", "artifact://tenant-ca/evidence/set-11",
                "a".repeat(64), EVIDENCE_READY, OPTIONAL_EVIDENCE_MISSING,
                Set.of("inventory"), Set.of("orders"), Set.of(), receivedAt);
    }

    @TestConfiguration
    static class TestBeans {
        @Bean
        @Primary
        FailOnceAfterState failOnceAfterState() {
            return new FailOnceAfterState();
        }

        @Bean
        @Primary
        AdjustableClock processManagerClock() {
            return new AdjustableClock(NOW.plusSeconds(7));
        }
    }

    static final class FailOnceAfterState implements AfterProcessStateHook {
        private final AtomicBoolean fail = new AtomicBoolean();

        void failNext() {
            fail.set(true);
        }

        void reset() {
            fail.set(false);
        }

        @Override
        public void afterStateMutation(String tenantId, String runId, long newVersion) {
            if (fail.getAndSet(false)) {
                throw new IllegalStateException("forced crash after state mutation");
            }
        }
    }

    static final class AdjustableClock extends Clock {
        private final AtomicReference<Instant> now;

        AdjustableClock(Instant initial) {
            now = new AtomicReference<>(initial);
        }

        void set(Instant instant) {
            now.set(instant);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) {
                throw new UnsupportedOperationException("test clock is fixed to UTC");
            }
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
