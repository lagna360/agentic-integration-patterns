package dev.agenticintegrationpatterns.orderdesk.approval;

import dev.agenticintegrationpatterns.orderdesk.OrderExceptionApplication;
import dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt;
import dev.agenticintegrationpatterns.orderdesk.effect.ExecuteEffect;
import dev.agenticintegrationpatterns.orderdesk.effect.FixtureInventoryReservationClient;
import dev.agenticintegrationpatterns.orderdesk.effect.ReserveInventoryEffect;
import dev.agenticintegrationpatterns.orderdesk.process.JdbcDurableProcessManager;
import dev.agenticintegrationpatterns.orderdesk.process.StartInvestigationRun;
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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static dev.agenticintegrationpatterns.orderdesk.approval.ApprovalDecision.Action.*;
import static dev.agenticintegrationpatterns.orderdesk.approval.ApprovalReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.approval.ApprovalReceipt.State.*;
import static dev.agenticintegrationpatterns.orderdesk.approval.ApprovalSubject.RiskClass.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(PolicyAndApprovalTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class PolicyAndApprovalTest {
    private static final Instant NOW = Instant.parse("2026-08-24T06:17:10Z");
    private static final String TENANT = "tenant-ca";
    private static final String RUN = "run-4c52e781-0838-35ee-84cc-7e59c537ad9c";
    private static final String CASE = "case-d5a30e20-f10b-38ca-9198-4834746bd37b";
    private static final String REQUEST = "approval-request-019492";

    @Autowired JdbcApprovalService approvals;
    @Autowired GuardedEffectService effects;
    @Autowired dev.agenticintegrationpatterns.orderdesk.effect.JdbcEffectLedger ledger;
    @Autowired JdbcDurableProcessManager process;
    @Autowired FixtureInventoryReservationClient client;
    @Autowired JdbcTemplate jdbc;
    @Autowired ProducerTemplate producer;
    @Autowired AdjustableClock clock;
    @Autowired FailingApprovalHook hook;

    @BeforeEach
    void reset() {
        jdbc.update("delete from authorized_effect");
        jdbc.update("delete from effect_outbox");
        jdbc.update("delete from effect_reconciliation_observation");
        jdbc.update("delete from effect_attempt");
        jdbc.update("delete from effect_identity_collision");
        jdbc.update("delete from effect_ledger");
        jdbc.update("delete from approval_outbox");
        jdbc.update("delete from approval_decision");
        jdbc.update("delete from approval_identity_collision");
        jdbc.update("delete from approval_message_inbox");
        jdbc.update("delete from approval_request");
        jdbc.update("delete from process_outbox");
        jdbc.update("delete from process_message_rejection");
        jdbc.update("delete from process_message_inbox");
        jdbc.update("delete from investigation_expected_work");
        jdbc.update("delete from investigation_run");
        clock.set(NOW);
        hook.reset();
        client.reset();
        process.start(new StartInvestigationRun(
                "start-15", TENANT, RUN, CASE, "corr-order-73051", "cmd-019483",
                "parallel-plan-v1", NOW.plusSeconds(367), Set.of("inventory"),
                Set.of("inventory"), NOW));
    }

    @Test
    // tag::canonical-human-approval-test[]
    void canonicalHumanDecisionBindsAuthorityBeforeOneEffectCall() {
        var subject = subject("", ALTERNATE_WAREHOUSE_SPLIT);
        var opened = approvals.open(REQUEST, subject);
        assertThat(opened.state()).isEqualTo(PENDING);
        assertThat(client.callCount()).isZero();

        var approved = approvals.decide(
                decision("approval-decision-019493", REQUEST, 0, APPROVE), approver("ops-lead"));
        assertThat(approved.state()).isEqualTo(APPROVED);
        assertThat(approved.authorityRef()).isEqualTo("approval://tenant-ca/approval-request-019492@v1");

        producer.requestBody("direct:register-authorized-effect",
                new RegisterAuthorizedEffect(REQUEST, subject, effect("")), EffectReceipt.class);
        var result = producer.requestBody("direct:execute-authorized-effect", execute(""), EffectReceipt.class);

        assertThat(result.state()).isEqualTo(EffectReceipt.State.SUCCEEDED);
        assertThat(client.callCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from authorized_effect", Integer.class))
                .isEqualTo(1);
    }
    // end::canonical-human-approval-test[]

    @Test
    void policyAutoAuthorizesButForbiddenAndIndeterminateFailClosed() {
        assertThat(approvals.open("auto", subject("-auto", LOW)).state())
                .isEqualTo(AUTO_AUTHORIZED);
        assertThat(approvals.open("forbidden",
                subject("-forbid", ApprovalSubject.RiskClass.FORBIDDEN)).state())
                .isEqualTo(ApprovalReceipt.State.FORBIDDEN);
        assertThat(approvals.open("unknown", subject("-unknown", UNKNOWN)).state())
                .isEqualTo(INDETERMINATE);

        assertThatThrownBy(() -> effects.registerAuthorized(
                new RegisterAuthorizedEffect("forbidden",
                        subject("-forbid", ApprovalSubject.RiskClass.FORBIDDEN),
                        effect("-forbid"))))
                .isInstanceOf(EffectAuthorityDeniedException.class);
        assertThat(client.callCount()).isZero();
    }

    @Test
    void trustedActorRoleAndSeparationOfDutiesAreRequired() {
        approvals.open(REQUEST, subject("", ALTERNATE_WAREHOUSE_SPLIT));

        assertThat(approvals.decide(decision("d-no-role", REQUEST, 0, APPROVE),
                new TrustedApproverContext(TENANT, "user:no-role", Set.of(), "sso:mfa")).disposition())
                .isEqualTo(DENIED);
        assertThat(approvals.decide(decision("d-self", REQUEST, 0, APPROVE),
                new TrustedApproverContext(TENANT, "workload:case-manager",
                        Set.of("ORDER_EXCEPTION_APPROVER"),
                        "sso:mfa")).disposition()).isEqualTo(DENIED);
        assertThat(approvals.current(TENANT, REQUEST).state()).isEqualTo(PENDING);
        assertThat(client.callCount()).isZero();
    }

    @Test
    void decisionsAreIdempotentAndChangedContentCollides() {
        approvals.open(REQUEST, subject("", ALTERNATE_WAREHOUSE_SPLIT));
        var actor = approver("one");
        var decision = decision("approval-decision-019493", REQUEST, 0, APPROVE);

        assertThat(approvals.decide(decision, actor).disposition()).isEqualTo(APPLIED);
        assertThat(approvals.decide(decision, actor).disposition()).isEqualTo(DUPLICATE_SAME);
        assertThat(approvals.decide(
                decision("approval-decision-019493", REQUEST, 0, REJECT), actor).disposition())
                .isEqualTo(IDENTITY_COLLISION);
        assertThat(jdbc.queryForObject(
                "select count(*) from approval_decision", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from approval_identity_collision", Integer.class)).isEqualTo(1);
    }

    @Test
    void staleAndConcurrentReviewersCannotOverwriteOneAnother() throws Exception {
        approvals.open(REQUEST, subject("", HIGH));
        assertThat(approvals.decide(decision("d-stale", REQUEST, 7, APPROVE), approver("stale"))
                .disposition()).isEqualTo(STALE_VERSION);

        var pool = Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> approvals.decide(
                    decision("d-a", REQUEST, 0, APPROVE), approver("a")));
            var second = pool.submit(() -> approvals.decide(
                    decision("d-b", REQUEST, 0, APPROVE), approver("b")));
            var dispositions = Set.of(first.get().disposition(), second.get().disposition());
            assertThat(dispositions).contains(APPLIED, STALE_VERSION);
        } finally {
            pool.shutdownNow();
        }
        var afterOne = approvals.current(TENANT, REQUEST);
        assertThat(afterOne.state()).isEqualTo(PENDING);
        assertThat(approvals.decide(
                decision("d-c", REQUEST, afterOne.version(), APPROVE), approver("c")).state())
                .isEqualTo(APPROVED);
    }

    @Test
    void theSameReviewerCannotSatisfyATwoPersonThresholdTwice() {
        approvals.open(REQUEST, subject("", HIGH));
        var actor = approver("one");
        assertThat(approvals.decide(decision("d-first", REQUEST, 0, APPROVE), actor).state())
                .isEqualTo(PENDING);
        assertThat(approvals.decide(decision("d-second", REQUEST, 1, APPROVE), actor).state())
                .isEqualTo(PENDING);
        assertThat(jdbc.queryForObject("""
                select event_type from approval_outbox
                 where request_id=? and approval_version=1
                """, String.class, REQUEST)).isEqualTo("ApprovalContributionRecorded");
        assertThat(approvals.decide(decision("d-third", REQUEST, 2, APPROVE), approver("two")).state())
                .isEqualTo(APPROVED);
    }

    @Test
    void concurrentDeliveryOfTheSameDecisionIdIsClassifiedIdempotently() throws Exception {
        approvals.open(REQUEST, subject("", ALTERNATE_WAREHOUSE_SPLIT));
        var message = decision("approval-decision-019493", REQUEST, 0, APPROVE);
        var actor = approver("one");
        var start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> {
                start.await();
                return approvals.decide(message, actor).disposition();
            });
            var second = pool.submit(() -> {
                start.await();
                return approvals.decide(message, actor).disposition();
            });
            start.countDown();
            assertThat(Set.of(first.get(), second.get())).containsExactlyInAnyOrder(
                    APPLIED, DUPLICATE_SAME);
        } finally {
            pool.shutdownNow();
        }
        assertThat(jdbc.queryForObject(
                "select count(*) from approval_decision", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from approval_message_inbox", Integer.class)).isEqualTo(1);
    }

    @Test
    void rejectChangeEscalateRevokeAndSupersedeRemainDistinct() {
        assertTransition("reject", "-reject", REJECT, REJECTED);
        assertTransition("change", "-change", REQUEST_CHANGE, CHANGES_REQUESTED);

        var escalatedSubject = subject("-escalate", ALTERNATE_WAREHOUSE_SPLIT);
        approvals.open("escalate", escalatedSubject);
        assertThat(approvals.decide(decision("d-escalate", "escalate", 0, ESCALATE),
                approver("one")).state()).isEqualTo(ESCALATED);

        var revokedSubject = subject("-revoke", ALTERNATE_WAREHOUSE_SPLIT);
        approvals.open("revoke", revokedSubject);
        approvals.decide(decision("d-approve-r", "revoke", 0, APPROVE), approver("one"));
        assertThat(approvals.decide(decision("d-revoke-denied", "revoke", 1, REVOKE),
                approver("two")).disposition()).isEqualTo(DENIED);
        assertThat(approvals.decide(decision("d-revoke", "revoke", 1, REVOKE),
                revoker()).state()).isEqualTo(REVOKED);

        approvals.open("supersede", subject("-supersede", ALTERNATE_WAREHOUSE_SPLIT));
        var admin = new TrustedApproverContext(TENANT, "user:policy-admin",
                Set.of("ORDER_POLICY_ADMIN"), "sso:mfa");
        assertThat(approvals.decide(decision("d-super", "supersede", 0, SUPERSEDE), admin).state())
                .isEqualTo(SUPERSEDED);
    }

    @Test
    void expiryAndChangedEvidencePreventRegistrationOrDispatch() {
        var subject = subject("", ALTERNATE_WAREHOUSE_SPLIT);
        approvals.open(REQUEST, subject);
        approvals.decide(decision("approval-decision-019493", REQUEST, 0, APPROVE), approver("one"));

        var changed = new ApprovalSubject(
                subject.tenantId(), subject.runId(), subject.caseId(), subject.proposalEventId(),
                subject.proposalId(), subject.category(), subject.effectId(), subject.warehouseId(),
                subject.sku(), subject.quantity(), subject.effectIntentSha256(),
                subject.evidenceSetRef(), "b".repeat(64), subject.contextSnapshotId(),
                subject.configurationRefs(), subject.incrementalShippingCostMinor(), subject.currency(),
                subject.evidenceValidUntil(), subject.proposerRef(), subject.subjectDigestVersion(),
                subject.riskClass());
        assertThatThrownBy(() -> effects.registerAuthorized(
                new RegisterAuthorizedEffect(REQUEST, changed, effect(""))))
                .isInstanceOf(EffectAuthorityDeniedException.class);

        effects.registerAuthorized(new RegisterAuthorizedEffect(REQUEST, subject, effect("")));
        clock.set(subject.evidenceValidUntil());
        assertThatThrownBy(() -> effects.executeAuthorized(execute("")))
                .isInstanceOf(EffectAuthorityDeniedException.class);
        assertThat(client.callCount()).isZero();
    }

    @Test
    void effectIntentDigestMustMatchTheExactLedgerIntent() {
        var subject = subject("", ALTERNATE_WAREHOUSE_SPLIT);
        approvals.open(REQUEST, subject);
        approvals.decide(decision("approval-decision-019493", REQUEST, 0, APPROVE), approver("one"));

        var changedPolicyBinding = new ReserveInventoryEffect(
                TENANT, RUN, CASE, "effect-reserve-13",
                "proposal://tenant-ca/proposal-019491",
                "policy://tenant-ca/order-effects/v4", "yyz-02", "camera-battery-x2", 2);
        assertThatThrownBy(() -> effects.registerAuthorized(
                new RegisterAuthorizedEffect(REQUEST, subject, changedPolicyBinding)))
                .isInstanceOf(EffectAuthorityDeniedException.class);
        assertThat(jdbc.queryForObject("select count(*) from effect_ledger", Integer.class)).isZero();
        assertThat(client.callCount()).isZero();
    }

    @Test
    void revocationAfterEffectRegistrationIsRecheckedBeforeDispatch() {
        var subject = subject("-revoked-dispatch", ALTERNATE_WAREHOUSE_SPLIT);
        approvals.open("revoked-dispatch", subject);
        approvals.decide(decision("d-authorize-dispatch", "revoked-dispatch", 0, APPROVE),
                approver("one"));
        effects.registerAuthorized(new RegisterAuthorizedEffect(
                "revoked-dispatch", subject, effect("-revoked-dispatch")));
        approvals.decide(decision("d-revoke-dispatch", "revoked-dispatch", 1, REVOKE),
                revoker());

        assertThatThrownBy(() -> effects.executeAuthorized(execute("-revoked-dispatch")))
                .isInstanceOf(EffectAuthorityDeniedException.class);
        assertThat(client.callCount()).isZero();
        assertThat(jdbc.queryForObject("select attempt_count from effect_ledger where effect_id=?",
                Integer.class, "effect-reserve-13-revoked-dispatch")).isZero();
    }

    @Test
    void dispatchRejectsAnAuthorityAggregateVersionDifferentFromRegistration() {
        var subject = subject("-version", ALTERNATE_WAREHOUSE_SPLIT);
        approvals.open("version", subject);
        approvals.decide(decision("d-version", "version", 0, APPROVE), approver("one"));
        effects.registerAuthorized(new RegisterAuthorizedEffect(
                "version", subject, effect("-version")));
        jdbc.update("""
                update approval_request set version=version+1
                 where tenant_id=? and request_id=?
                """, TENANT, "version");

        assertThatThrownBy(() -> effects.executeAuthorized(execute("-version")))
                .isInstanceOf(EffectAuthorityDeniedException.class);
        assertThat(client.callCount()).isZero();
    }

    @Test
    // tag::zero-effect-without-authority-test[]
    void missingRejectedOrRevokedAuthorityProducesZeroTargetCalls() {
        assertThatThrownBy(() -> effects.executeAuthorized(execute("")))
                .isInstanceOf(EffectAuthorityDeniedException.class);

        var subject = subject("", ALTERNATE_WAREHOUSE_SPLIT);
        approvals.open(REQUEST, subject);
        assertThatThrownBy(() -> effects.registerAuthorized(
                new RegisterAuthorizedEffect(REQUEST, subject, effect(""))))
                .isInstanceOf(EffectAuthorityDeniedException.class);
        assertThat(client.callCount()).isZero();
    }
    // end::zero-effect-without-authority-test[]

    @Test
    void approvalStateAndOutboxIntentRollBackTogether() {
        approvals.open(REQUEST, subject("", ALTERNATE_WAREHOUSE_SPLIT));
        hook.failOn(APPROVED);

        assertThatThrownBy(() -> approvals.decide(
                decision("approval-decision-019493", REQUEST, 0, APPROVE), approver("one")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("forced approval-state crash");

        assertThat(approvals.current(TENANT, REQUEST).state()).isEqualTo(PENDING);
        assertThat(jdbc.queryForObject("select count(*) from approval_decision", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from approval_outbox", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void expiryWaitsForTheServerOwnedBoundaryWithoutRenewingTheRun() {
        approvals.open(REQUEST, subject("", ALTERNATE_WAREHOUSE_SPLIT));
        var early = approvals.expire(TENANT, REQUEST, 0);
        assertThat(early.disposition()).isEqualTo(NOT_DUE);
        assertThat(early.state()).isEqualTo(PENDING);

        clock.set(NOW.plusSeconds(67));
        var expired = approvals.expire(TENANT, REQUEST, 0);
        assertThat(expired.state()).isEqualTo(ApprovalReceipt.State.EXPIRED);
        assertThat(jdbc.queryForObject("""
                select event_payload from approval_outbox
                 where request_id=? and approval_version=1
                """, String.class, REQUEST)).contains("DECISION_DUE_EXPIRED");
        assertThat(jdbc.queryForObject("select deadline_at from investigation_run where tenant_id=? and run_id=?",
                Instant.class, TENANT, RUN)).isEqualTo(NOW.plusSeconds(367));
    }

    private void assertTransition(
            String request, String suffix, ApprovalDecision.Action action, ApprovalReceipt.State state) {
        approvals.open(request, subject(suffix, ALTERNATE_WAREHOUSE_SPLIT));
        assertThat(approvals.decide(decision("d-" + request, request, 0, action), approver(request)).state())
                .isEqualTo(state);
    }

    private ApprovalSubject subject(String suffix, ApprovalSubject.RiskClass risk) {
        var effect = effect(suffix);
        return new ApprovalSubject(
                TENANT, RUN, CASE, "evt-019491" + suffix, "proposal-019491" + suffix,
                "SPLIT_SHIPMENT", "effect-reserve-13" + suffix, "yyz-02", "camera-battery-x2", 2,
                ledger.intentSha256(effect), "artifact://tenant-ca/evidence/set-018", "a".repeat(64),
                "snapshot-8c65c449-da5e-3075-a0b6-fd444f8bd1f0",
                java.util.List.of("instruction://order-exception-investigation-v1",
                        "policy://order-exception-ca-17",
                        "capabilities://order-desk-capabilities-v1"),
                costFor(risk), "CAD", NOW.plusSeconds(67),
                "workload:case-manager", "approval-subject-v1", risk);
    }

    private ReserveInventoryEffect effect(String suffix) {
        return new ReserveInventoryEffect(
                TENANT, RUN, CASE, "effect-reserve-13" + suffix,
                "proposal://tenant-ca/proposal-019491" + suffix,
                "policy://tenant-ca/order-effects/v3", "yyz-02", "camera-battery-x2", 2);
    }

    private ExecuteEffect execute(String suffix) {
        return new ExecuteEffect(TENANT, "effect-reserve-13" + suffix,
                "approval-effect-worker", Duration.ofSeconds(5));
    }

    private static ApprovalDecision decision(
            String id, String request, long version, ApprovalDecision.Action action) {
        return new ApprovalDecision(id, request, version, action, "OPERATOR_DECISION");
    }

    private static TrustedApproverContext approver(String suffix) {
        return new TrustedApproverContext(
                TENANT, "user:order-approver:" + suffix,
                Set.of("ORDER_EXCEPTION_APPROVER"), "sso:mfa");
    }

    private static TrustedApproverContext revoker() {
        return new TrustedApproverContext(
                TENANT, "user:approval-revoker",
                Set.of("ORDER_APPROVAL_REVOKER"), "sso:mfa");
    }

    private static long costFor(ApprovalSubject.RiskClass risk) {
        return switch (risk) {
            case LOW -> 1_000;
            case HIGH -> 12_000;
            default -> 3_200;
        };
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestBeans {
        @Bean
        @Primary
        AdjustableClock approvalTestClock() {
            return new AdjustableClock(NOW);
        }

        @Bean
        @Primary
        FailingApprovalHook failingApprovalHook() {
            return new FailingApprovalHook();
        }
    }

    static final class FailingApprovalHook implements AfterApprovalStateHook {
        private ApprovalReceipt.State failState;

        void failOn(ApprovalReceipt.State state) {
            failState = state;
        }

        void reset() {
            failState = null;
        }

        @Override
        public void afterStateMutation(
                String tenantId, String requestId, ApprovalReceipt.State state, long version) {
            if (state == failState) {
                throw new IllegalStateException("forced approval-state crash");
            }
        }
    }

    static final class AdjustableClock extends Clock {
        private volatile Instant instant;

        AdjustableClock(Instant instant) {
            this.instant = instant;
        }

        void set(Instant instant) {
            this.instant = instant;
        }

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return instant; }
    }
}
