package dev.agenticintegrationpatterns.orderdesk.recovery;

import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
import dev.agenticintegrationpatterns.orderdesk.approval.*;
import dev.agenticintegrationpatterns.orderdesk.effect.*;
import dev.agenticintegrationpatterns.orderdesk.process.JdbcDurableProcessManager;
import dev.agenticintegrationpatterns.orderdesk.process.StartInvestigationRun;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.recovery.ResolutionPlanDefinition.Reversibility.*;
import static dev.agenticintegrationpatterns.orderdesk.recovery.ResolutionReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.recovery.ResolutionReceipt.State.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(ResolutionRecoveryTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class ResolutionRecoveryTest {
    private static final Instant NOW = Instant.parse("2026-08-26T22:00:00Z");
    private static final String TENANT = "tenant-ca";
    private static final String RUN = "run-4c52e781-0838-35ee-84cc-7e59c537ad9c";
    private static final String CASE = "case-d5a30e20-f10b-38ca-9198-4834746bd37b";
    private static final String PLAN = "resolution-plan-019494";
    private static final String RESERVE = "effect-reserve-13";
    private static final String SHIPMENT = "effect-create-split-shipment-16";
    private static final String RELEASE = "effect-release-reserve-16";
    private static final String EVIDENCE_SHA = "a".repeat(64);
    private static final String CONFIG = "configuration://order-effects-ca@v7";

    @Autowired JdbcResolutionRecoveryManager manager;
    @Autowired JdbcApprovalService approvals;
    @Autowired GuardedEffectService guardedEffects;
    @Autowired JdbcEffectLedger ledger;
    @Autowired EffectExecutionService execution;
    @Autowired JdbcDurableProcessManager process;
    @Autowired FixtureSplitShipmentClient shipmentClient;
    @Autowired FixtureInventoryReservationReleaseClient releaseClient;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired ProducerTemplate producer;
    @Autowired AdjustableClock clock;
    @Autowired FailingResolutionHook hook;

    @BeforeEach
    void reset() {
        clear();
        clock.set(NOW);
        hook.reset();
        shipmentClient.reset();
        releaseClient.reset();
        process.start(new StartInvestigationRun(
                "start-16", TENANT, RUN, CASE, "corr-order-73051", "cmd-019483",
                "parallel-plan-v1", NOW.plusSeconds(600), Set.of("inventory"),
                Set.of("inventory"), NOW));
        var reservation = reservation();
        var subject = approvalSubject(reservation);
        approvals.open("approval-request-019492", subject);
        approvals.decide(new ApprovalDecision(
                        "approval-decision-019493", "approval-request-019492", 0,
                        ApprovalDecision.Action.APPROVE, "OPERATOR_DECISION"),
                new TrustedApproverContext(TENANT, "user:order-approver:canonical",
                        Set.of("ORDER_EXCEPTION_APPROVER"), "sso:mfa"));
        guardedEffects.registerAuthorized(new RegisterAuthorizedEffect(
                "approval-request-019492", subject, reservation));
        ledger.register(shipment());
    }

    @AfterEach
    void cleanup() {
        clear();
    }

    @Test
    // tag::later-step-failure-compensation-test[]
    void laterStepFailureSelectsAndCompletesOneGovernedCompensation() {
        assertThat(manager.open(plan(COMPENSATABLE)).state()).isEqualTo(FORWARD_RUNNING);

        executeReserve();
        assertThat(observe("msg-reserve", RESERVE).state()).isEqualTo(FORWARD_RUNNING);

        shipmentClient.nextResultWillBeConfirmedFailure();
        execution.executeSplitShipment(execute(SHIPMENT));
        assertThat(shipmentClient.lastExpectedOrderVersion()).isEqualTo(41);
        assertThat(jdbc.queryForObject(
                "select resolution_evidence_ref from effect_ledger where effect_id=?",
                String.class, SHIPMENT)).contains("ORDER_VERSION_MISMATCH");
        assertThat(observe("msg-shipment-failed", SHIPMENT).state())
                .isEqualTo(RECOVERY_DECISION_REQUIRED);

        // tag::canonical-compensated-history-test[]
        assertThat(manager.selectCompensation(selectRecovery()).state())
                .isEqualTo(COMPENSATION_PENDING);
        assertThat(ledger.snapshot(TENANT, RELEASE).compensatesEffectId()).isEqualTo(RESERVE);
        assertThat(ledger.snapshot(TENANT, RELEASE).authorityRef())
                .isEqualTo("recovery-authority-019498");

        execution.executeRelease(execute(RELEASE));
        assertThat(observe("msg-release-succeeded", RELEASE).state()).isEqualTo(COMPENSATED);
        assertThat(releaseClient.callCount()).isEqualTo(1);
        assertThat(jdbc.queryForList(
                "select event_id from resolution_outbox order by event_id", String.class))
                .containsExactly("evt-019494", "evt-019495", "evt-019496", "evt-019497",
                        "evt-019498", "evt-019499", "evt-019500");
        // end::canonical-compensated-history-test[]
        assertThat(jdbc.queryForList(
                "select event_type from resolution_outbox order by event_id", String.class))
                .containsExactly("ResolutionPlanStarted", "ResolutionEffectObserved",
                        "ResolutionEffectObserved", "RecoverySelected",
                        "RecoveryAuthorityIssued", "CompensationSucceeded",
                        "ResolutionPlanCompensated");

        assertThat(assertEvent("evt-019494", "ResolutionPlanStarted"))
                .containsEntry("caseId", CASE)
                .containsEntry("proposalId", "proposal-019491")
                .containsEntry("proposalEventId", "evt-019491")
                .containsEntry("causedByEventId", "evt-019491");
        assertThat(assertEvent("evt-019494", "ResolutionPlanStarted")
                .get("forwardEffectIds")).asList().containsExactly(RESERVE, SHIPMENT);
        assertThat(assertEvent("evt-019495", "ResolutionEffectObserved"))
                .containsEntry("effectId", RESERVE)
                .containsEntry("observedEffectState", "SUCCEEDED")
                .containsEntry("observedEffectVersion", 2)
                .containsEntry("targetReference", "rsv-8842")
                .containsEntry("causedByEventId", "evt-019494");
        assertThat(assertEvent("evt-019496", "ResolutionEffectObserved"))
                .containsEntry("effectId", SHIPMENT)
                .containsEntry("observedEffectState", "FAILED_CONFIRMED")
                .containsEntry("observedEffectVersion", 2)
                .containsEntry("causedByEffectId", RESERVE)
                .containsEntry("causedByEventId", "evt-019495");
        assertThat(assertEvent("evt-019496", "ResolutionEffectObserved")
                .get("targetEvidenceRef").toString()).contains("ORDER_VERSION_MISMATCH");
        assertThat(assertEvent("evt-019497", "RecoverySelected"))
                .containsEntry("selectedAction", "RELEASE_INVENTORY_RESERVATION")
                .containsEntry("effectId", RELEASE)
                .containsEntry("compensatesEffectId", RESERVE)
                .containsEntry("failedEffectId", SHIPMENT)
                .containsEntry("causedByEventId", "evt-019496");
        assertThat(assertEvent("evt-019498", "RecoveryAuthorityIssued"))
                .containsEntry("authorityRef", "recovery-authority-019498")
                .containsEntry("authorityPlanVersion", 2)
                .containsEntry("failedEffectId", SHIPMENT)
                .containsEntry("failedEffectVersion", 2)
                .containsEntry("evidenceSha256", EVIDENCE_SHA)
                .containsEntry("configurationRef", CONFIG)
                .containsEntry("causedByEventId", "evt-019497");
        assertThat(assertEvent("evt-019499", "CompensationSucceeded"))
                .containsEntry("effectId", RELEASE)
                .containsEntry("compensatesEffectId", RESERVE)
                .containsEntry("observedEffectState", "SUCCEEDED")
                .containsEntry("causedByEventId", "evt-019498");
        assertThat(assertEvent("evt-019500", "ResolutionPlanCompensated"))
                .containsEntry("effectId", RELEASE)
                .containsEntry("compensatesEffectId", RESERVE)
                .containsEntry("state", "COMPENSATED")
                .containsEntry("causedByEventId", "evt-019499");
    }
    // end::later-step-failure-compensation-test[]

    @Test
    void duplicateAndChangedSignalsDoNotRedrawPlanHistory() {
        manager.open(plan(COMPENSATABLE));
        executeReserve();
        var signal = signal("msg-reserve", RESERVE, "warehouse-receipt");

        assertThat(manager.observe(signal).disposition()).isEqualTo(APPLIED);
        assertThat(manager.observe(signal).disposition()).isEqualTo(DUPLICATE_SAME);
        assertThat(manager.observe(signal("msg-reserve", RESERVE, "changed-evidence"))
                .disposition()).isEqualTo(IDENTITY_COLLISION);
        assertThat(manager.current(TENANT, PLAN).version()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from resolution_outbox", Integer.class)).isEqualTo(2);
    }

    @Test
    void acceptedLaterEffectRequiresObservationAndBlocksCompensation() {
        manager.open(plan(COMPENSATABLE));
        executeReserve();
        observe("msg-reserve", RESERVE);
        shipmentClient.nextResultWillBeAccepted();
        execution.executeSplitShipment(execute(SHIPMENT));

        assertThat(observe("msg-shipment-accepted", SHIPMENT).state())
                .isEqualTo(OBSERVATION_REQUIRED);
        assertThatThrownBy(() -> manager.selectCompensation(selectRecovery()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not eligible");
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_ledger where effect_id=?",
                Integer.class, RELEASE)).isZero();
    }

    @Test
    void staleOrExpiredRecoveryAuthorityCannotCreateAnEffect() {
        advanceToConfirmedFailure();
        var wrongPremise = authority(
                EVIDENCE_SHA, NOW.plusSeconds(300), 1, 1);
        assertThatThrownBy(() -> manager.selectCompensation(
                new SelectReservationReleaseRecovery(TENANT, PLAN, SHIPMENT,
                        release(wrongPremise))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("locked recovery premise");

        var staleAuthority = authority("b".repeat(64), NOW.plusSeconds(300));
        assertThatThrownBy(() -> manager.selectCompensation(
                new SelectReservationReleaseRecovery(TENANT, PLAN, SHIPMENT,
                        release(staleAuthority))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stale");

        clock.set(NOW.plusSeconds(301));
        assertThatThrownBy(() -> manager.selectCompensation(selectRecovery()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("expired");
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_ledger where effect_id=?",
                Integer.class, RELEASE)).isZero();
    }

    @Test
    void failedCompensationProducesTruthfulManualRecovery() {
        advanceToConfirmedFailure();
        manager.selectCompensation(selectRecovery());
        releaseClient.nextResultWillBeConfirmedFailure();
        execution.executeRelease(execute(RELEASE));

        assertThat(observe("msg-release-failed", RELEASE).state()).isEqualTo(MANUAL_RECOVERY);
        assertThat(ledger.snapshot(TENANT, RESERVE).state()).isEqualTo(EffectReceipt.State.SUCCEEDED);
        assertThat(ledger.snapshot(TENANT, RELEASE).state())
                .isEqualTo(EffectReceipt.State.FAILED_CONFIRMED);
    }

    @Test
    void lostCompensationReplyBlocksBlindSecondReleaseUntilReconciliation() {
        advanceToConfirmedFailure();
        manager.selectCompensation(selectRecovery());
        releaseClient.nextReplyWillBeUnknown();

        var unknown = execution.executeRelease(execute(RELEASE));
        assertThat(unknown.state()).isEqualTo(EffectReceipt.State.UNKNOWN);
        assertThat(observe("msg-release-unknown", RELEASE).state())
                .isEqualTo(OBSERVATION_REQUIRED);

        var duplicate = execution.executeRelease(execute(RELEASE));
        assertThat(duplicate.state()).isEqualTo(EffectReceipt.State.UNKNOWN);
        assertThat(releaseClient.callCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_attempt where effect_id=?",
                Integer.class, RELEASE)).isEqualTo(1);
    }

    @Test
    void irreversibleSuccessFollowedByFailureRequiresManualRecovery() {
        manager.open(plan(IRREVERSIBLE));
        executeReserve();
        observe("msg-reserve", RESERVE);
        shipmentClient.nextResultWillBeConfirmedFailure();
        execution.executeSplitShipment(execute(SHIPMENT));

        assertThat(observe("msg-shipment-failed", SHIPMENT).state()).isEqualTo(MANUAL_RECOVERY);
        assertThatThrownBy(() -> manager.selectCompensation(selectRecovery()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    // tag::recovery-crash-rollback-test[]
    void crashAfterStateMutationRollsBackStepInboxStateAndOutboxThenRedeliveryRecovers() {
        manager.open(plan(COMPENSATABLE));
        executeReserve();
        observe("msg-reserve", RESERVE);
        shipmentClient.nextResultWillBeConfirmedFailure();
        execution.executeSplitShipment(execute(SHIPMENT));
        hook.failOn(RECOVERY_DECISION_REQUIRED);

        assertThatThrownBy(() -> manager.observe(signal(
                "msg-shipment-failed", SHIPMENT, "effect-ledger://" + SHIPMENT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("forced resolution-state crash");
        assertThat(manager.current(TENANT, PLAN).state()).isEqualTo(FORWARD_RUNNING);
        assertThat(jdbc.queryForObject(
                "select count(*) from resolution_message_inbox where message_id=?",
                Integer.class, "msg-shipment-failed")).isZero();
        assertThat(jdbc.queryForObject(
                "select count(*) from resolution_outbox", Integer.class)).isEqualTo(2);

        hook.reset();
        assertThat(observe("msg-shipment-failed", SHIPMENT).state())
                .isEqualTo(RECOVERY_DECISION_REQUIRED);
    }
    // end::recovery-crash-rollback-test[]

    @Test
    void recoveryAuthorityIsRecheckedImmediatelyBeforeDispatch() {
        advanceToConfirmedFailure();
        manager.selectCompensation(selectRecovery());
        clock.set(NOW.plusSeconds(301));

        assertThatThrownBy(() -> execution.executeRelease(execute(RELEASE)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("expired");
        assertThat(releaseClient.callCount()).isZero();
        assertThat(ledger.snapshot(TENANT, RELEASE).state()).isEqualTo(EffectReceipt.State.RECORDED);
        assertThat(manager.current(TENANT, PLAN).state()).isEqualTo(MANUAL_RECOVERY);
        assertThat(assertEvent("evt-019499", "RecoveryAuthorityInvalidated"))
                .containsEntry("effectId", RELEASE)
                .containsEntry("reasonCode", "RECOVERY_AUTHORITY_EXPIRED")
                .containsEntry("causedByEventId", "evt-019498");

        var refused = manager.current(TENANT, PLAN);
        assertThatThrownBy(() -> execution.executeRelease(execute(RELEASE)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recovery_plan_not_pending");
        assertThat(releaseClient.callCount()).isZero();
        assertThat(manager.current(TENANT, PLAN).version()).isEqualTo(refused.version());
        assertThat(jdbc.queryForObject("""
                select count(*) from resolution_outbox
                 where event_type='RecoveryAuthorityInvalidated'
                """, Integer.class)).isOne();
    }

    @Test
    void splitShipmentAuthorityIsRecheckedImmediatelyBeforeDispatch() {
        manager.open(plan(COMPENSATABLE));
        executeReserve();
        observe("msg-reserve", RESERVE);
        clock.set(NOW.plusSeconds(600));

        assertThatThrownBy(() -> execution.executeSplitShipment(execute(SHIPMENT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("expired");
        assertThat(shipmentClient.callCount()).isZero();
        assertThat(ledger.snapshot(TENANT, SHIPMENT).state())
                .isEqualTo(EffectReceipt.State.RECORDED);
    }

    @Test
    void outOfOrderLaterStepSignalIsRetainedWithoutChangingPlanHistory() {
        manager.open(plan(COMPENSATABLE));
        var original = signal(
                "msg-shipment-early", SHIPMENT, "caller://untrusted/early");

        var result = manager.observe(original);

        assertThat(result.disposition()).isEqualTo(OUT_OF_ORDER);
        assertThat(result.version()).isZero();
        assertThat(jdbc.queryForObject(
                "select count(*) from resolution_outbox", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select disposition from resolution_message_inbox where message_id=?",
                String.class, "msg-shipment-early")).isEqualTo("OUT_OF_ORDER");

        executeReserve();
        observe("msg-reserve", RESERVE);
        shipmentClient.nextResultWillBeConfirmedFailure();
        execution.executeSplitShipment(execute(SHIPMENT));
        assertThat(manager.observe(original).disposition()).isEqualTo(OUT_OF_ORDER);

        var redrive = signal(
                "msg-shipment-redrive-1", SHIPMENT, "caller://untrusted/redrive");
        assertThat(manager.redriveOutOfOrder(new RedriveEffectOutcome(original, redrive)).state())
                .isEqualTo(RECOVERY_DECISION_REQUIRED);
        assertThat(jdbc.queryForObject(
                "select count(*) from resolution_outbox", Integer.class)).isEqualTo(3);
    }

    @Test
    void callerEvidencePointerCannotReplaceLedgerOwnedOutcomeEvidence() {
        manager.open(plan(COMPENSATABLE));
        executeReserve();

        manager.observe(signal("msg-hostile-pointer", RESERVE,
                "https://attacker.invalid/not-authoritative"));

        String owned = ledger.snapshot(TENANT, RESERVE).resolutionEvidenceRef();
        assertThat(jdbc.queryForObject("""
                select observation_evidence_ref from resolution_plan_effect
                 where tenant_id=? and plan_id=? and effect_id=?
                """, String.class, TENANT, PLAN, RESERVE)).isEqualTo(owned);
        assertThat(assertEvent("evt-019495", "ResolutionEffectObserved"))
                .containsEntry("targetEvidenceRef", owned)
                .doesNotContainValue("https://attacker.invalid/not-authoritative");
    }

    @Test
    void unknownLaterEffectBlocksCompensationAndMakesNoRecoveryCall() {
        manager.open(plan(COMPENSATABLE));
        executeReserve();
        observe("msg-reserve", RESERVE);
        shipmentClient.nextReplyWillBeUnknown();
        execution.executeSplitShipment(execute(SHIPMENT));

        assertThat(observe("msg-shipment-unknown", SHIPMENT).state())
                .isEqualTo(OBSERVATION_REQUIRED);
        assertThatThrownBy(() -> manager.selectCompensation(selectRecovery()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(releaseClient.callCount()).isZero();
        assertThat(jdbc.queryForObject(
                "select count(*) from effect_ledger where effect_id=?",
                Integer.class, RELEASE)).isZero();
    }

    @Test
    void planIdentityCollisionAndCrossPlanMessageReuseFailClosed() {
        assertThat(manager.open(plan(COMPENSATABLE)).disposition()).isEqualTo(CREATED);
        var changed = new ResolutionPlanDefinition(
                TENANT, PLAN, RUN, CASE, "proposal-changed", "evt-019491",
                "order-exception-remediation",
                NOW.plusSeconds(600), "artifact://tenant-ca/evidence/set-018", EVIDENCE_SHA,
                CONFIG, 19_494, plan(COMPENSATABLE).effects());
        assertThat(manager.open(changed).disposition()).isEqualTo(IDENTITY_COLLISION);

        executeReserve();
        manager.observe(signal("shared-message", RESERVE, "ledger-evidence"));
        assertThatThrownBy(() -> manager.observe(new EffectOutcomeSignal(
                TENANT, "shared-message", "missing-plan", RESERVE, "ledger-evidence")))
                .isInstanceOf(
                        JdbcResolutionRecoveryManager.ResolutionMessageIdentityCollisionException.class);
    }

    private void advanceToConfirmedFailure() {
        manager.open(plan(COMPENSATABLE));
        executeReserve();
        observe("msg-reserve", RESERVE);
        shipmentClient.nextResultWillBeConfirmedFailure();
        execution.executeSplitShipment(execute(SHIPMENT));
        observe("msg-shipment-failed", SHIPMENT);
    }

    private ResolutionReceipt observe(String message, String effect) {
        return producer.requestBody("direct:observe-resolution-effect",
                signal(message, effect, "effect-ledger://" + effect), ResolutionReceipt.class);
    }

    private EffectOutcomeSignal signal(String message, String effect, String evidence) {
        return new EffectOutcomeSignal(TENANT, message, PLAN, effect, evidence);
    }

    private ResolutionPlanDefinition plan(ResolutionPlanDefinition.Reversibility firstStep) {
        return new ResolutionPlanDefinition(
                TENANT, PLAN, RUN, CASE, "proposal-019491", "evt-019491",
                "order-exception-remediation",
                NOW.plusSeconds(600), "artifact://tenant-ca/evidence/set-018", EVIDENCE_SHA,
                CONFIG, 19_494, List.of(
                new ResolutionPlanDefinition.ForwardEffect(
                        1, RESERVE, "RESERVE_INVENTORY", null,
                        "warehouse-reservation-v1",
                        "approval://tenant-ca/approval-request-019492@v1",
                        NOW.plusSeconds(600), firstStep),
                new ResolutionPlanDefinition.ForwardEffect(
                        2, SHIPMENT, "CREATE_SPLIT_SHIPMENT", RESERVE,
                        "order-split-shipment-v1",
                        "policy-authority://tenant-ca/resolution-plan-019494/create-split-shipment@v1",
                        NOW.plusSeconds(600), CORRECTIVE_FORWARD_ONLY)));
    }

    private ReserveInventoryEffect reservation() {
        return new ReserveInventoryEffect(
                TENANT, RUN, CASE, RESERVE, "proposal://tenant-ca/proposal-019491",
                "policy://tenant-ca/order-effects/v3", "yyz-02", "camera-battery-x2", 2);
    }

    private CreateSplitShipmentEffect shipment() {
        return new CreateSplitShipmentEffect(
                TENANT, RUN, CASE, SHIPMENT, "resolution-plan://tenant-ca/" + PLAN,
                "policy://tenant-ca/order-effects/v3", RESERVE,
                "policy-authority://tenant-ca/resolution-plan-019494/create-split-shipment@v1",
                NOW.plusSeconds(600), EVIDENCE_SHA, CONFIG, "ord-73051", 41, "rsv-8842",
                "yyz-02", "camera-battery-x2", 2);
    }

    private SelectReservationReleaseRecovery selectRecovery() {
        return new SelectReservationReleaseRecovery(
                TENANT, PLAN, SHIPMENT, release(authority(EVIDENCE_SHA, NOW.plusSeconds(300))));
    }

    private ReleaseInventoryReservationEffect release(RecoveryAuthority authority) {
        return new ReleaseInventoryReservationEffect(
                TENANT, RUN, CASE, PLAN, RELEASE, SHIPMENT, RESERVE,
                "recovery-decision://tenant-ca/evt-019497",
                "policy://tenant-ca/recovery/v2", "warehouse-reservation-release-v1",
                "rsv-8842", "yyz-02", "camera-battery-x2", 2, authority);
    }

    private RecoveryAuthority authority(String evidenceSha, Instant validUntil) {
        return authority(evidenceSha, validUntil, 2, 2);
    }

    private RecoveryAuthority authority(
            String evidenceSha, Instant validUntil,
            long planVersion, long failedEffectVersion) {
        return new RecoveryAuthority(
                "recovery-authority-019498", TENANT, PLAN, planVersion,
                SHIPMENT, failedEffectVersion, RELEASE, RESERVE,
                "RELEASE_INVENTORY_RESERVATION",
                "warehouse/yyz-02/reservations/rsv-8842",
                "policy://tenant-ca/recovery/v2", "LATER_STEP_CONFIRMED_FAILED",
                evidenceSha, CONFIG, validUntil);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> assertEvent(String eventId, String eventType) {
        try {
            String payload = jdbc.queryForObject("""
                    select event_payload from resolution_outbox
                     where event_id=? and event_type=?
                    """, String.class, eventId, eventType);
            var event = mapper.readValue(payload, Map.class);
            assertThat(event)
                    .containsEntry("eventId", eventId)
                    .containsEntry("eventType", eventType)
                    .containsEntry("tenantId", TENANT)
                    .containsEntry("planId", PLAN);
            return event;
        } catch (Exception failure) {
            throw new AssertionError("cannot read resolution event " + eventId, failure);
        }
    }

    private ExecuteEffect execute(String effectId) {
        return new ExecuteEffect(TENANT, effectId, "recovery-worker", Duration.ofSeconds(10));
    }

    private EffectReceipt executeReserve() {
        return guardedEffects.executeAuthorized(execute(RESERVE));
    }

    private ApprovalSubject approvalSubject(ReserveInventoryEffect effect) {
        return new ApprovalSubject(
                TENANT, RUN, CASE, "evt-019491", "proposal-019491", "SPLIT_SHIPMENT",
                RESERVE, "yyz-02", "camera-battery-x2", 2,
                ledger.intentSha256(effect), "artifact://tenant-ca/evidence/set-018",
                EVIDENCE_SHA, "snapshot-8c65c449-da5e-3075-a0b6-fd444f8bd1f0",
                List.of("instruction://order-exception-investigation-v1",
                        "policy://order-exception-ca-17",
                        "capabilities://order-desk-capabilities-v1"),
                3_200, "CAD", NOW.plusSeconds(600), "workload:case-manager",
                "approval-subject-v1", ApprovalSubject.RiskClass.ALTERNATE_WAREHOUSE_SPLIT);
    }

    private void clear() {
        jdbc.update("delete from resolution_outbox");
        jdbc.update("delete from resolution_message_inbox");
        jdbc.update("delete from resolution_plan_effect");
        jdbc.update("delete from resolution_plan");
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
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestBeans {
        @Bean @Primary AdjustableClock recoveryClock() { return new AdjustableClock(NOW); }
        @Bean @Primary FailingResolutionHook resolutionHook() { return new FailingResolutionHook(); }
    }

    static final class FailingResolutionHook implements AfterResolutionStateHook {
        private ResolutionReceipt.State failState;
        void failOn(ResolutionReceipt.State state) { failState = state; }
        void reset() { failState = null; }
        @Override
        public void afterStateMutation(
                String tenantId, String planId, ResolutionReceipt.State state, long version) {
            if (state == failState) {
                throw new IllegalStateException("forced resolution-state crash");
            }
        }
    }

    static final class AdjustableClock extends Clock {
        private Instant instant;
        AdjustableClock(Instant instant) { this.instant = instant; }
        void set(Instant instant) { this.instant = instant; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return instant; }
    }
}
