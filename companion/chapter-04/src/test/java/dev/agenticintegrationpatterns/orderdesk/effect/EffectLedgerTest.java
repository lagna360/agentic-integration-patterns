package dev.agenticintegrationpatterns.orderdesk.effect;

import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
import dev.agenticintegrationpatterns.orderdesk.process.JdbcDurableProcessManager;
import dev.agenticintegrationpatterns.orderdesk.process.RunState;
import dev.agenticintegrationpatterns.orderdesk.process.StartInvestigationRun;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt.State.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(EffectLedgerTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class EffectLedgerTest {
    private static final Instant NOW = Instant.parse("2026-08-26T15:00:00Z");

    @Autowired JdbcEffectLedger ledger;
    @Autowired EffectExecutionService execution;
    @Autowired FixtureInventoryReservationClient client;
    @Autowired JdbcDurableProcessManager processManager;
    @Autowired JdbcTemplate jdbc;
    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;
    @Autowired AdjustableClock clock;
    @Autowired FailOnEffectState hook;

    @BeforeEach
    void reset() {
        deleteEffectData();
        jdbc.update("delete from process_outbox");
        jdbc.update("delete from process_message_rejection");
        jdbc.update("delete from process_message_inbox");
        jdbc.update("delete from investigation_expected_work");
        jdbc.update("delete from investigation_run");
        clock.set(NOW);
        hook.reset();
        client.reset();
        processManager.start(new StartInvestigationRun(
                "start-13", "tenant-ca", "run-13", "case-A", "corr-13", "cmd-13",
                "parallel-plan-v1", NOW.plusSeconds(60),
                Set.of("inventory"), Set.of("inventory"), NOW));
    }

    @AfterEach
    void removeEffectRowsSharedByTheNamedTeachingDatabase() {
        deleteEffectData();
    }

    private void deleteEffectData() {
        jdbc.update("delete from effect_outbox");
        jdbc.update("delete from effect_reconciliation_observation");
        jdbc.update("delete from effect_attempt");
        jdbc.update("delete from effect_identity_collision");
        jdbc.update("delete from effect_ledger");
    }

    @Test
    // tag::effect-identity-test[]
    void duplicateIntentIsOneEffectButChangedIntentIsACollision() {
        assertThat(ledger.register(effect(2)).disposition()).isEqualTo(CREATED);
        assertThat(ledger.register(effect(2)).disposition()).isEqualTo(DUPLICATE_SAME);
        assertThat(ledger.register(effect(3)).disposition()).isEqualTo(IDENTITY_COLLISION);

        assertThat(jdbc.queryForObject(
                "select count(*) from effect_ledger", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_outbox", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_identity_collision", Integer.class)).isEqualTo(1);
        assertThat(client.callCount()).isZero();
    }
    // end::effect-identity-test[]

    @Test
    void effectAndItsOutboxEventRollBackTogether() {
        hook.failOn(RECORDED);

        assertThatThrownBy(() -> ledger.register(effect(2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("forced effect-state crash");

        assertThat(jdbc.queryForObject(
                "select count(*) from effect_ledger", Integer.class)).isZero();
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_outbox", Integer.class)).isZero();
    }

    @Test
    void duplicateExecutionDeliveryDoesNotCallTheTargetTwice() {
        ledger.register(effect(2));

        var first = producer.requestBody("direct:execute-recorded-effect",
                execute(), EffectReceipt.class);
        var duplicate = producer.requestBody("direct:execute-recorded-effect",
                execute(), EffectReceipt.class);

        assertThat(first.state()).isEqualTo(SUCCEEDED);
        assertThat(duplicate.disposition()).isEqualTo(NO_EXECUTION);
        assertThat(duplicate.state()).isEqualTo(SUCCEEDED);
        assertThat(client.callCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_attempt", Integer.class)).isEqualTo(1);
    }

    @Test
    // tag::lost-reply-reconciliation-test[]
    void committedTargetWithLostReplyBecomesUnknownAndMustBeReconciled() {
        var recorded = ledger.register(effect(2));
        client.nextReplyWillBeLostAfterCommit();

        var unknown = producer.requestBody("direct:execute-recorded-effect",
                execute(), EffectReceipt.class);
        var blockedDuplicate = producer.requestBody("direct:execute-recorded-effect",
                execute(), EffectReceipt.class);

        assertThat(unknown.state()).isEqualTo(UNKNOWN);
        assertThat(blockedDuplicate.state()).isEqualTo(UNKNOWN);
        assertThat(client.callCount()).isEqualTo(1);
        assertThat(consumer.receive("seda:effect-reconciliation", 2_000)).isNotNull();

        var reconciled = execution.reconcile(new ReconcileEffect(
                "tenant-ca", "effect-reserve-13", "reconcile-1"));
        assertThat(reconciled.state()).isEqualTo(SUCCEEDED);
        assertThat(reconciled.targetIdempotencyKey())
                .isEqualTo(recorded.targetIdempotencyKey());
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_reconciliation_observation",
                Integer.class)).isEqualTo(1);
    }
    // end::lost-reply-reconciliation-test[]

    @Test
    void asynchronousAcceptanceIsNotCompletion() {
        var recorded = ledger.register(effect(2));
        client.nextResultWillBeAcceptedAsynchronously();

        var accepted = execution.executeOne(execute());

        assertThat(accepted.state()).isEqualTo(ACCEPTED);
        assertThat(execution.executeOne(execute()).state()).isEqualTo(ACCEPTED);
        assertThat(client.callCount()).isEqualTo(1);

        client.resolveAcceptedAsSucceeded(
                "tenant-ca", recorded.targetIdempotencyKey());
        var completed = execution.reconcile(new ReconcileEffect(
                "tenant-ca", "effect-reserve-13", "reconcile-accepted"));
        assertThat(completed.state()).isEqualTo(SUCCEEDED);
    }

    @Test
    void reconciliationDeliveryIsIdempotentButChangedContentIsACollision() {
        ledger.register(effect(2));
        client.nextReplyWillBeLostAfterCommit();
        assertThat(execution.executeOne(execute()).state()).isEqualTo(UNKNOWN);

        var command = new ReconcileEffect(
                "tenant-ca", "effect-reserve-13", "reconcile-duplicate");
        var first = execution.reconcile(command);
        var duplicate = execution.reconcile(command);

        assertThat(first.state()).isEqualTo(SUCCEEDED);
        assertThat(duplicate.state()).isEqualTo(SUCCEEDED);
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_reconciliation_observation",
                Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_outbox", Integer.class)).isEqualTo(4);

        assertThatThrownBy(() -> ledger.reconcile(
                "tenant-ca", "effect-reserve-13", "reconcile-duplicate",
                new InventoryReservationClient.TargetObservation(
                        InventoryReservationClient.Outcome.FAILED_CONFIRMED, null,
                        "warehouse-query:definitive-non-application")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity collision");
    }

    @Test
    void targetAuthoritativeNonApplicationIsConfirmedFailure() {
        ledger.register(effect(2));
        client.nextResultWillBeConfirmedNotApplied();

        var failed = execution.executeOne(execute());

        assertThat(failed.state()).isEqualTo(FAILED_CONFIRMED);
        assertThat(failed.state()).isNotEqualTo(UNKNOWN);
    }

    @Test
    // tag::abandoned-attempt-test[]
    void expiredDispatchClaimBecomesUnknownWithoutCallingTheTarget() {
        ledger.register(effect(2));
        ledger.claimRecorded(
                "tenant-ca", "effect-reserve-13", "dead-worker",
                Duration.ofSeconds(5)).orElseThrow();
        clock.set(NOW.plusSeconds(6));

        var recovered = ledger.recoverExpiredDispatch(
                "tenant-ca", "effect-reserve-13", "worker-heartbeat-expired")
                .orElseThrow();

        assertThat(recovered.state()).isEqualTo(UNKNOWN);
        assertThat(ledger.claimRecorded(
                "tenant-ca", "effect-reserve-13", "new-worker",
                Duration.ofSeconds(5))).isEmpty();
        assertThat(client.callCount()).isZero();
    }
    // end::abandoned-attempt-test[]

    @Test
    void targetSuccessFollowedByLocalCrashRecoversThroughTargetEvidence() {
        ledger.register(effect(2));
        hook.failOn(SUCCEEDED);

        assertThatThrownBy(() -> execution.executeOne(execute()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("forced effect-state crash");

        assertThat(client.callCount()).isEqualTo(1);
        assertThat(ledger.current("tenant-ca", "effect-reserve-13").state())
                .isEqualTo(DISPATCHING);
        clock.set(NOW.plusSeconds(11));
        assertThat(ledger.recoverExpiredDispatch(
                "tenant-ca", "effect-reserve-13", "local-commit-missing")
                .orElseThrow().state()).isEqualTo(UNKNOWN);

        var reconciled = execution.reconcile(new ReconcileEffect(
                "tenant-ca", "effect-reserve-13", "reconcile-after-crash"));
        assertThat(reconciled.state()).isEqualTo(SUCCEEDED);
        assertThat(client.callCount()).isEqualTo(1);
    }

    @Test
    void unexpectedAdapterDefectEscapesAndLeavesARecoverableDispatchRecord() {
        ledger.register(effect(2));
        var broken = new EffectExecutionService(ledger, new InventoryReservationClient() {
            @Override
            public InvocationResult reserve(ReservationRequest request) {
                throw new NullPointerException("fixture defect");
            }

            @Override
            public TargetObservation findByIdempotencyKey(
                    String tenantId, String targetIdempotencyKey) {
                throw new UnsupportedOperationException();
            }
        });

        assertThatThrownBy(() -> broken.executeOne(execute()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fixture defect");
        assertThat(ledger.current("tenant-ca", "effect-reserve-13").state())
                .isEqualTo(DISPATCHING);
    }

    @Test
    void malformedTargetReceiptIsRejectedAndLeavesARecoverableDispatchRecord() {
        ledger.register(effect(2));
        var malformed = new EffectExecutionService(ledger,
                new InventoryReservationClient() {
                    @Override
                    public InvocationResult reserve(ReservationRequest request) {
                        return new InvocationResult(
                                Outcome.SUCCEEDED, "reservation/13", " ");
                    }

                    @Override
                    public TargetObservation findByIdempotencyKey(
                            String tenantId, String targetIdempotencyKey) {
                        throw new UnsupportedOperationException();
                    }
                });

        assertThatThrownBy(() -> malformed.executeOne(execute()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("evidenceRef");
        assertThat(ledger.current("tenant-ca", "effect-reserve-13").state())
                .isEqualTo(DISPATCHING);
    }

    @Test
    void oversizedTargetReferenceIsRejectedBeforeTheSqlEvidenceBoundary() {
        ledger.register(effect(2));
        var malformed = new EffectExecutionService(ledger,
                new InventoryReservationClient() {
                    @Override
                    public InvocationResult reserve(ReservationRequest request) {
                        return new InvocationResult(
                                InventoryReservationClient.Outcome.FAILED_CONFIRMED,
                                "r".repeat(601), "target-rejection:13");
                    }

                    @Override
                    public TargetObservation findByIdempotencyKey(
                            String tenantId, String targetIdempotencyKey) {
                        throw new UnsupportedOperationException();
                    }
                });

        assertThatThrownBy(() -> malformed.executeOne(execute()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetReference");
        assertThat(ledger.current("tenant-ca", "effect-reserve-13").state())
                .isEqualTo(DISPATCHING);
    }

    @Test
    void commandIdentifiersAreBoundedBeforeTheyReachSql() {
        assertThatThrownBy(() -> new ReconcileEffect(
                "t".repeat(121), "effect-reserve-13", "observation-13"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tenantId");
        assertThatThrownBy(() -> new ReserveInventoryEffect(
                "tenant-ca", "run-13", "case-A", "effect-reserve-13",
                "decision://13", "policy://13",
                "w".repeat(160), "s".repeat(160), 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetResourceKey");
    }

    @Test
    void stoppedRunDoesNotEraseItsUnknownEffectObligation() {
        ledger.register(effect(2));
        client.nextReplyWillBeLostAfterCommit();
        assertThat(execution.executeOne(execute()).state()).isEqualTo(UNKNOWN);

        clock.set(NOW.plusSeconds(61));
        var deadlineLease = processManager.claimNextDue(
                "deadline-worker", Duration.ofSeconds(10)).orElseThrow();
        var stopped = processManager.stopAtDeadline("deadline-13", deadlineLease);

        assertThat(stopped.state()).isEqualTo(RunState.STOPPED);
        assertThat(ledger.current("tenant-ca", "effect-reserve-13").state())
                .isEqualTo(UNKNOWN);
    }

    private ReserveInventoryEffect effect(int quantity) {
        return new ReserveInventoryEffect(
                "tenant-ca", "run-13", "case-A", "effect-reserve-13",
                "decision://tenant-ca/case-A/resolution-7",
                "policy://tenant-ca/order-effects/v3",
                "warehouse-east", "SKU-4242", quantity);
    }

    private ExecuteEffect execute() {
        return new ExecuteEffect(
                "tenant-ca", "effect-reserve-13", "effect-worker-A",
                Duration.ofSeconds(10));
    }

    @TestConfiguration
    static class TestBeans {
        @Bean
        @Primary
        AdjustableClock effectLedgerClock() {
            return new AdjustableClock(NOW);
        }

        @Bean
        @Primary
        FailOnEffectState failOnEffectState() {
            return new FailOnEffectState();
        }
    }

    static final class FailOnEffectState implements AfterEffectStateHook {
        private final AtomicReference<EffectReceipt.State> failOn =
                new AtomicReference<>();

        void failOn(EffectReceipt.State state) {
            failOn.set(state);
        }

        void reset() {
            failOn.set(null);
        }

        @Override
        public void afterStateMutation(
                String tenantId,
                String effectId,
                EffectReceipt.State state,
                long version) {
            if (failOn.compareAndSet(state, null)) {
                throw new IllegalStateException(
                        "forced effect-state crash at " + state);
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
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) {
                throw new UnsupportedOperationException(
                        "test clock is fixed to UTC");
            }
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
