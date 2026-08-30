package dev.agenticintegrationpatterns.orderdesk.peer;

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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(RemotePeerTaskManagerTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class RemotePeerTaskManagerTest {
    private static final Instant NOW = Instant.parse("2026-08-27T15:00:00Z");
    private static final String TENANT = "tenant-ca";
    private static final String WORK = "remote-work-18-01";
    private static final String PEER = "peer:carrier-cross-border";
    private static final String TASK = "carrier-task-8842";
    private static final String SHA = "a".repeat(64);

    @Autowired ProducerTemplate producer;
    @Autowired JdbcTemplate jdbc;
    @Autowired FixtureProtectedPeerRegistry registry;
    @Autowired FixtureProtectedRemotePeerContextProvider contexts;
    @Autowired JdbcRemotePeerTaskManager manager;
    @Autowired AdjustableClock clock;
    @Autowired ForcedRemotePeerRaceHook raceHook;

    @BeforeEach
    void reset() {
        jdbc.update("delete from remote_peer_outbox");
        jdbc.update("delete from remote_peer_artifact");
        jdbc.update("delete from remote_peer_message_inbox");
        jdbc.update("delete from remote_peer_task");
        registry.reset();
        clock.set(NOW);
        raceHook.reset();
        contexts.useForTest(context(PEER, TENANT));
    }

    @Test
    // tag::untrusted-advertisement-test[]
    void advertisementCannotAuthorizeAnUnregisteredOrIncompatiblePeer() {
        assertThatThrownBy(() -> open()).hasRootCauseMessage("PEER_NOT_REGISTERED");
        assertThat(count("remote_peer_task")).isZero();
        assertThat(count("remote_peer_outbox")).isZero();

        registry.put(registration("A2A", "1.0", "evidence-task-v1"));
        RemoteTaskDefinition drift = definition(advertisement("A2A", "1.1", "evidence-task-v1"));
        assertThatThrownBy(() -> producer.requestBody(
                "direct:open-remote-peer-task", drift, RemoteTaskReceipt.class))
                .hasRootCauseMessage("NO_COMPATIBLE_PEER_CONTRACT");
        assertThat(count("remote_peer_task")).isZero();
    }
    // end::untrusted-advertisement-test[]

    @Test
    void protectedRegistrationPinsTheContractAndFixedAdapter() {
        register();
        RemoteTaskReceipt opened = open();

        assertThat(opened.taskState()).isEqualTo("DISPATCH_PENDING");
        assertThat(value("fixed_adapter_ref")).isEqualTo("adapter:carrier-specialist-fixed");
        assertThat(value("fixed_adapter_ref")).isNotEqualTo("https://advertised.invalid/a2a");
        assertThat(count("remote_peer_outbox")).isOne();
    }

    @Test
    void authenticatedAcceptanceBindsOpaquePeerTaskIdentity() {
        register(); open();

        RemoteTaskReceipt receipt = send(update("msg-18-01", RemotePeerUpdate.Kind.ACCEPTED,
                null, null, null, 100, 2_000, null));

        assertThat(receipt.taskState()).isEqualTo("REMOTE_RUNNING");
        assertThat(receipt.peerTaskId()).isEqualTo(TASK);
    }

    @Test
    // tag::partial-and-late-test[]
    void partialEvidenceStaysPartialAndLateCompletionCannotReopenTimedOutWork() {
        register(); open();
        send(update("msg-18-partial", RemotePeerUpdate.Kind.PARTIAL,
                "artifact-part-18", "artifact://tenant-ca/carrier/part-18", SHA,
                200, 3_000, "evidence-bundle"));

        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("REMOTE_RUNNING");
        assertThat(jdbc.queryForObject("select artifact_role from remote_peer_artifact",
                String.class)).isEqualTo("PARTIAL");

        clock.set(NOW.plusSeconds(121));
        assertThat(manager.expireDue()).isOne();
        RemoteTaskReceipt late = send(update("msg-18-late", RemotePeerUpdate.Kind.COMPLETED,
                "artifact-final-18", "artifact://tenant-ca/carrier/final-18", SHA,
                300, 4_000, null));

        assertThat(late.disposition()).isEqualTo("LATE");
        assertThat(late.taskState()).isEqualTo("TIMED_OUT");
        assertThat(count("remote_peer_artifact")).isOne();
    }
    // end::partial-and-late-test[]

    @Test
    void duplicateIsAbsorbedButChangedContentUnderTheSameMessageIdIsContained() {
        register(); open();
        RemotePeerUpdate accepted = update("msg-18-dup", RemotePeerUpdate.Kind.ACCEPTED,
                null, null, null, 100, 1_000, null);

        assertThat(send(accepted).disposition()).isEqualTo("ACCEPTED");
        assertThat(send(accepted).disposition()).isEqualTo("DUPLICATE");
        RemotePeerUpdate changed = update("msg-18-dup", RemotePeerUpdate.Kind.ACCEPTED,
                null, null, null, 101, 1_000, null);
        assertThat(send(changed).taskState()).isEqualTo("CONTAINED");
    }

    @Test
    void changedContentAfterCompletionRecordsCollisionWithoutReopeningTerminalWork() {
        register(); open();
        RemotePeerUpdate completed = update("msg-18-terminal-collision",
                RemotePeerUpdate.Kind.COMPLETED, "artifact-final-18",
                "artifact://tenant-ca/carrier/final-18", SHA, 500, 5_000, null);
        assertThat(send(completed).taskState()).isEqualTo("COMPLETED");
        assertThat(send(completed).disposition()).isEqualTo("DUPLICATE");

        RemotePeerUpdate changed = update("msg-18-terminal-collision",
                RemotePeerUpdate.Kind.ACCEPTED, null, null, null, 501, 5_000, null);
        RemoteTaskReceipt collision = send(changed);

        assertThat(collision.disposition()).isEqualTo("COLLISION_RECORDED");
        assertThat(collision.taskState()).isEqualTo("COMPLETED");
        assertThat(eventCount("RemotePeerMessageCollisionDetected")).isOne();
        assertThat(count("remote_peer_artifact")).isOne();
    }

    @Test
    // tag::protocol-drift-test[]
    void protocolDriftWrongPeerAndRemoteEffectReportNeverBecomeEvidenceOrEffects() {
        register(); open();
        contexts.useForTest(context("peer:impostor", TENANT));
        assertThat(send(update("msg-18-wrong-peer", RemotePeerUpdate.Kind.ACCEPTED,
                null, null, null, 0, 0, null)).reasonCode())
                .isEqualTo("AUTHENTICATED_PEER_MISMATCH");
        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("DISPATCH_PENDING");

        contexts.useForTest(context(PEER, TENANT));
        RemotePeerUpdate drift = new RemotePeerUpdate("msg-18-drift", TENANT, WORK, TASK,
                RemotePeerUpdate.Kind.ACCEPTED, "A2A", "1.1", "evidence-task-v1",
                0, 0, null, null, null, null,
                null, null, null, null, Set.of(), null);
        assertThat(send(drift).taskState()).isEqualTo("CONTAINED");
        assertThat(count("remote_peer_artifact")).isZero();

        reset(); register(); open();
        RemotePeerUpdate effect = update("msg-18-effect", RemotePeerUpdate.Kind.COMPLETED,
                "artifact-final-18", "artifact://tenant-ca/carrier/final-18", SHA,
                100, 1_000, null, "effect://carrier/label-purchased");
        assertThat(send(effect).reasonCode())
                .isEqualTo("REMOTE_EFFECT_REQUIRES_MANUAL_OWNERSHIP");
        assertThat(count("effect_outbox")).isZero();
        assertThat(count("effect_attempt")).isZero();
    }
    // end::protocol-drift-test[]

    @Test
    // tag::cancellation-semantics-test[]
    void cancellationRequestIsIntentUntilTheBoundPeerAcknowledgesIt() {
        register(); open();
        send(update("msg-18-accept", RemotePeerUpdate.Kind.ACCEPTED,
                null, null, null, 50, 1_000, null));

        RemoteTaskReceipt requested = producer.requestBody(
                "direct:request-remote-peer-cancellation",
                new RemoteCancellationRequest(TENANT, WORK, "CASE_NO_LONGER_NEEDS_RESULT"),
                RemoteTaskReceipt.class);
        assertThat(requested.taskState()).isEqualTo("CANCELLATION_REQUESTED");
        assertThat(requested.reasonCode()).contains("NOT_CONFIRMED_STOP");

        RemoteTaskReceipt confirmed = send(update("msg-18-cancelled",
                RemotePeerUpdate.Kind.CANCELLED, null, null, null, 50, 1_000, null));
        assertThat(confirmed.taskState()).isEqualTo("CANCELLED_CONFIRMED");
    }
    // end::cancellation-semantics-test[]

    @Test
    void finalEvidenceCompletesLocallyWithoutAnyConsequentialEffect() {
        register(); open();
        RemoteTaskReceipt completed = send(update("msg-18-complete",
                RemotePeerUpdate.Kind.COMPLETED, "artifact-final-18",
                "artifact://tenant-ca/carrier/final-18", SHA, 500, 5_000, null));

        assertThat(completed.taskState()).isEqualTo("COMPLETED");
        assertThat(count("remote_peer_artifact")).isOne();
        assertThat(count("authorized_effect")).isZero();
        assertThat(count("effect_attempt")).isZero();
        assertThat(count("effect_outbox")).isZero();
    }

    @Test
    void reportedBudgetOverrunContainsLocallyButDoesNotClaimThePeerStopped() {
        register(); open();
        RemoteTaskReceipt receipt = send(update("msg-18-budget",
                RemotePeerUpdate.Kind.PARTIAL, null, null, null,
                1_001, 1_000, null));

        assertThat(receipt.taskState()).isEqualTo("CONTAINED");
        assertThat(receipt.reasonCode()).contains("DOES_NOT_PROVE_REMOTE_STOP");
        assertThat(jdbc.queryForObject("""
                select count(*) from remote_peer_outbox
                where event_type='RemotePeerCancellationRequested'
                """, Integer.class)).isOne();
    }

    @Test
    void failureAndInterruptionOutcomesHaveNamedLocalStates() {
        register(); open();
        assertThat(send(update("msg-18-input", RemotePeerUpdate.Kind.INPUT_REQUIRED,
                null, null, null, 10, 100, null)).taskState())
                .isEqualTo("WAITING_FOR_INPUT");
        clock.set(NOW.plusSeconds(121));
        assertThat(manager.expireDue()).isOne();
        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("TIMED_OUT");

        reset(); register(); open();
        assertThat(send(update("msg-18-auth", RemotePeerUpdate.Kind.AUTH_REQUIRED,
                null, null, null, 10, 100, null)).taskState())
                .isEqualTo("WAITING_FOR_PEER_AUTHORIZATION");
        clock.set(NOW.plusSeconds(121));
        assertThat(manager.expireDue()).isOne();
        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("TIMED_OUT");

        reset(); register(); open();
        assertThat(send(update("msg-18-failed", RemotePeerUpdate.Kind.FAILED,
                null, null, null, 10, 100, null)).taskState()).isEqualTo("FAILED");
    }

    @Test
    void negativeOrRegressingCumulativeUsageIsContained() {
        register(); open();
        send(update("msg-18-usage-1", RemotePeerUpdate.Kind.ACCEPTED,
                null, null, null, 100, 1_000, null));
        RemoteTaskReceipt regressed = send(update("msg-18-usage-2",
                RemotePeerUpdate.Kind.PARTIAL, null, null, null, 99, 1_000, null));
        assertThat(regressed.taskState()).isEqualTo("CONTAINED");
        assertThat(regressed.reasonCode()).contains("REGRESSING");

        reset(); register(); open();
        RemoteTaskReceipt negative = send(update("msg-18-usage-negative",
                RemotePeerUpdate.Kind.ACCEPTED, null, null, null, -1, 0, null));
        assertThat(negative.taskState()).isEqualTo("CONTAINED");
    }

    @Test
    void finalResultMustMatchThePinnedSchemaProvenanceAndDeliverables() {
        register(); open();
        assertThatThrownBy(() -> send(update("msg-18-missing",
                RemotePeerUpdate.Kind.COMPLETED, "artifact-final-18",
                "artifact://tenant-ca/carrier/final-18", SHA, 500, 5_000,
                "evidence-bundle")))
                .hasRootCauseMessage("FINAL_RESULT_HAS_MISSING_DELIVERABLES");
        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("DISPATCH_PENDING");
        assertThat(count("remote_peer_artifact")).isZero();
    }

    @Test
    void changedMessageIdContentContainsTheOriginalWorkNotAnUntrustedWorkReference() {
        register(); open();
        send(update("msg-18-cross-work", RemotePeerUpdate.Kind.ACCEPTED,
                null, null, null, 10, 100, null));
        String otherWork = "remote-work-18-02";
        open(otherWork);

        RemotePeerUpdate collision = updateForWork("msg-18-cross-work", otherWork,
                RemotePeerUpdate.Kind.ACCEPTED, 11, 100);
        contexts.useForTest(context("peer:impostor", TENANT));
        assertThat(send(collision).reasonCode()).isEqualTo("AUTHENTICATED_PEER_MISMATCH");
        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("REMOTE_RUNNING");

        contexts.useForTest(context(PEER, TENANT));
        RemoteTaskReceipt contained = send(collision);

        assertThat(contained.remoteWorkId()).isEqualTo(WORK);
        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("CONTAINED");
        assertThat(manager.current(TENANT, otherWork).taskState()).isEqualTo("DISPATCH_PENDING");
    }

    @Test
    void protectedContextIsBoundToTheExpectedGatewayService() {
        register(); open();
        contexts.useForTest(new ProtectedRemotePeerContext(PEER, Set.of(TENANT),
                JdbcRemotePeerTaskManager.AUDIENCE, "service:unexpected-gateway",
                NOW.plusSeconds(600)));

        assertThat(send(update("msg-18-service", RemotePeerUpdate.Kind.ACCEPTED,
                null, null, null, 0, 0, null)).reasonCode())
                .isEqualTo("WRONG_SERVICE_CONTEXT");
        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("DISPATCH_PENDING");
    }

    @Test
    void staleOwnerCannotPublishCancellationOrTimeoutEvents() {
        register(); open();
        raceHook.arm("CANCELLATION");
        assertThatThrownBy(() -> manager.requestCancellation(
                new RemoteCancellationRequest(TENANT, WORK, "TEST_RACE")))
                .hasMessage("CONCURRENT_REMOTE_TASK_UPDATE");
        assertThat(eventCount("RemotePeerCancellationRequested")).isZero();

        clock.set(NOW.plusSeconds(121));
        raceHook.arm("EXPIRY");
        assertThat(manager.expireDue()).isZero();
        assertThat(eventCount("RemotePeerWorkTimedOut")).isZero();

        reset(); register(); open();
        raceHook.arm("CONTAINMENT");
        assertThatThrownBy(() -> send(update("msg-18-budget-race",
                RemotePeerUpdate.Kind.PARTIAL, null, null, null,
                1_001, 1_000, null)))
                .hasRootCauseMessage("CONCURRENT_REMOTE_TASK_UPDATE");
        assertThat(eventCount("RemotePeerCancellationRequested")).isZero();
        assertThat(manager.current(TENANT, WORK).taskState()).isEqualTo("DISPATCH_PENDING");
    }

    private RemoteTaskReceipt open() {
        return open(WORK);
    }

    private RemoteTaskReceipt open(String remoteWorkId) {
        return producer.requestBody("direct:open-remote-peer-task", definition(advertisement(
                "A2A", "1.0", "evidence-task-v1"), remoteWorkId), RemoteTaskReceipt.class);
    }

    private RemoteTaskReceipt send(RemotePeerUpdate update) {
        return producer.requestBody("direct:accept-remote-peer-update", update,
                RemoteTaskReceipt.class);
    }

    private void register() {
        registry.put(registration("A2A", "1.0", "evidence-task-v1"));
    }

    private ProtectedPeerRegistration registration(
            String protocol, String version, String profile) {
        return new ProtectedPeerRegistration(PEER, Set.of(TENANT),
                "cross-border-shipment-assessment", protocol, version, profile,
                "adapter:carrier-specialist-fixed", 7, true);
    }

    private PeerAdvertisement advertisement(String protocol, String version, String profile) {
        return new PeerAdvertisement(PEER, Set.of("cross-border-shipment-assessment"),
                protocol, version, profile, "card://carrier/specialist@44",
                "https://advertised.invalid/a2a", NOW.plusSeconds(300));
    }

    private RemoteTaskDefinition definition(PeerAdvertisement advertisement) {
        return definition(advertisement, WORK);
    }

    private RemoteTaskDefinition definition(
            PeerAdvertisement advertisement, String remoteWorkId) {
        return new RemoteTaskDefinition(TENANT, remoteWorkId, "case-shipment-risk-18",
                "corr-shipment-risk-18", "service:order-desk-process-manager",
                "Assess cross-border route risk from the retained manifest",
                Set.of("applicable-restrictions", "route-cutoff-observations",
                        "evidence-bundle", "missing-deliverables"),
                "artifact://tenant-ca/shipment/manifest-18", SHA,
                NOW.plusSeconds(120), 1_000, 50_000, 8_192,
                "cross-border-shipment-assessment",
                "schema://carrier/cross-border-assessment@1", advertisement);
    }

    private RemotePeerUpdate update(String messageId, RemotePeerUpdate.Kind kind,
            String artifactId, String artifactRef, String artifactSha,
            long tokens, long cost, String missing) {
        return update(messageId, kind, artifactId, artifactRef, artifactSha,
                tokens, cost, missing, null);
    }

    private RemotePeerUpdate update(String messageId, RemotePeerUpdate.Kind kind,
            String artifactId, String artifactRef, String artifactSha,
            long tokens, long cost, String missing, String effectRef) {
        return new RemotePeerUpdate(messageId, TENANT, WORK, TASK, kind,
                "A2A", "1.0", "evidence-task-v1", tokens, cost,
                artifactId, artifactRef, artifactSha,
                artifactId == null ? null : 700,
                artifactId == null ? null : "schema://carrier/cross-border-assessment@1",
                artifactId == null ? null : "provenance://carrier/task-8842/run-1",
                artifactId == null ? null : NOW.minusSeconds(30),
                artifactId == null ? null : NOW.plusSeconds(300),
                missing == null ? Set.of() : Set.of(missing), effectRef);
    }

    private RemotePeerUpdate updateForWork(String messageId, String remoteWorkId,
            RemotePeerUpdate.Kind kind, long tokens, long cost) {
        return new RemotePeerUpdate(messageId, TENANT, remoteWorkId, TASK, kind,
                "A2A", "1.0", "evidence-task-v1", tokens, cost,
                null, null, null, null, null, null, null, null, Set.of(), null);
    }

    private static ProtectedRemotePeerContext context(String peer, String tenant) {
        return new ProtectedRemotePeerContext(peer, Set.of(tenant),
                JdbcRemotePeerTaskManager.AUDIENCE, "service:order-desk-peer-gateway",
                NOW.plusSeconds(600));
    }

    private int count(String table) {
        return jdbc.queryForObject("select count(*) from " + table, Integer.class);
    }

    private String value(String column) {
        return jdbc.queryForObject("select " + column + " from remote_peer_task", String.class);
    }

    private int eventCount(String eventType) {
        return jdbc.queryForObject("""
                select count(*) from remote_peer_outbox where event_type=?
                """, Integer.class, eventType);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestBeans {
        @Bean @Primary AdjustableClock remotePeerClock() {
            return new AdjustableClock(NOW);
        }

        @Bean @Primary ForcedRemotePeerRaceHook forcedRemotePeerRaceHook(JdbcTemplate jdbc) {
            return new ForcedRemotePeerRaceHook(jdbc);
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

    static final class ForcedRemotePeerRaceHook implements AfterRemotePeerTaskReadHook {
        private final JdbcTemplate jdbc;
        private String armedOperation;

        ForcedRemotePeerRaceHook(JdbcTemplate jdbc) { this.jdbc = jdbc; }
        void arm(String operation) { this.armedOperation = operation; }
        void reset() { this.armedOperation = null; }

        @Override
        public void afterRead(
                String operation, String tenantId, String remoteWorkId, long version) {
            if (!operation.equals(armedOperation)) return;
            armedOperation = null;
            jdbc.update("""
                    update remote_peer_task set version=version+1
                    where tenant_id=? and remote_work_id=? and version=?
                    """, tenantId, remoteWorkId, version);
        }
    }
}
