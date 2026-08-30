package dev.agenticintegrationpatterns.orderdesk.security;

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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.security.ProtectedRouteState.*;
import static dev.agenticintegrationpatterns.orderdesk.security.RouteAction.*;
import static dev.agenticintegrationpatterns.orderdesk.security.SecurityDecision.Outcome.ALLOW;
import static dev.agenticintegrationpatterns.orderdesk.security.SecurityDecision.Outcome.DENY;
import static dev.agenticintegrationpatterns.orderdesk.security.SecuredResourceSnapshot.State.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(RouteSecurityGateTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class RouteSecurityGateTest {
    private static final Instant NOW = Instant.parse("2026-08-27T14:00:00Z");
    private static final String CONSUMER = "workload:effect-gateway-consumer";
    private static final String RELAY = "workload:resolution-outbox-relay";

    @Autowired ProducerTemplate producer;
    @Autowired ProtectedRouteState state;
    @Autowired FixtureProtectedContextProvider contexts;
    @Autowired FixtureSecurityAdmissionSink sink;
    @Autowired JdbcTemplate jdbc;
    @Autowired FailingHook hook;

    @BeforeEach
    void reset() {
        jdbc.update("delete from route_security_quarantine");
        jdbc.update("delete from route_security_decision");
        state.reset(NOW);
        contexts.reset();
        sink.reset();
        hook.fail = false;
    }

    @Test
    // tag::secured-compensation-test[]
    void protectedProvenanceAndResourceStatePermitHandoffToTheExistingEffectBoundary() {
        SecurityDecision decision = send(canonical());

        assertThat(decision.outcome()).isEqualTo(ALLOW);
        assertThat(decision.reasonCode()).isEqualTo("CURRENT_ROUTE_POLICY_PERMITS");
        assertThat(decision.authenticatedWorkloadRef()).isEqualTo(CONSUMER);
        assertThat(decision.producerProvenanceRef())
                .isEqualTo("provenance://ingress/msg-compensation-dispatch-019501");
        assertThat(sink.handoffCount()).isOne();
    }
    // end::secured-compensation-test[]

    @Test
    // tag::producer-provenance-test[]
    void recordFieldsCannotManufactureTrustedProducerProvenance() {
        SecuredRouteMessage forged = admit(
                canonical(), "diagnostic-readmission-019501-1",
                "workload:diagnostic-replayer", TENANT);

        SecurityDecision decision = send(forged);

        assertThat(decision.outcome()).isEqualTo(DENY);
        assertThat(decision.reasonCode()).isEqualTo("PRODUCER_PROVENANCE_MISMATCH");
        assertThat(decision.authenticatedWorkloadRef()).isEqualTo(CONSUMER);
        assertThat(sink.handoffCount()).isZero();
        assertThat(SecuredRouteMessage.class.getRecordComponents())
                .extracting(component -> component.getName())
                .doesNotContain("protectedContext", "breakGlassGrant");
    }
    // end::producer-provenance-test[]

    @Test
    void changingASecurityRelevantFieldUnderExistingProvenanceIsDenied() {
        SecuredRouteMessage tampered = copy(canonical(),
                "workload:resolution-outbox-relay", TENANT, RESOURCE,
                "different-authority", 2, 1, null);

        SecurityDecision decision = send(tampered);

        assertThat(decision.outcome()).isEqualTo(DENY);
        assertThat(decision.reasonCode()).isEqualTo("PRODUCER_RECORD_INTEGRITY_MISMATCH");
        assertThat(sink.handoffCount()).isZero();
    }

    @Test
    // tag::cross-tenant-readmission-test[]
    void actualCrossTenantResourceIsDeniedFromProtectedResourceOwnership() {
        SecuredRouteMessage crossTenant = admit(copy(canonical(),
                "workload:resolution-outbox-relay", "tenant-us",
                "effect://tenant-us/effect-release-reserve-16", DELEGATION, 2, 1, null),
                "cross-tenant-019498", "workload:resolution-outbox-relay", "tenant-us");

        SecurityDecision decision = send(crossTenant);

        assertThat(decision.outcome()).isEqualTo(DENY);
        assertThat(decision.reasonCode()).isEqualTo("TENANT_BOUNDARY_MISMATCH");
        assertThat(sink.handoffCount()).isZero();
    }
    // end::cross-tenant-readmission-test[]

    @Test
    void staleWrongAudienceAndRevokedConsumerContextsAreDenied() {
        contexts.useForTest(context(CONSUMER, Set.of(TENANT),
                "wrong-audience", "WORKLOAD_MTLS", Set.of("EFFECT_GATEWAY"), null,
                NOW.plusSeconds(60)));
        assertThat(send(canonical()).reasonCode()).isEqualTo("WRONG_AUDIENCE");

        contexts.useForTest(context(CONSUMER, Set.of(TENANT),
                JdbcRouteSecurityGate.EFFECT_AUDIENCE, "WORKLOAD_MTLS",
                Set.of("EFFECT_GATEWAY"), null, NOW));
        assertThat(send(canonical()).reasonCode()).isEqualTo("STALE_AUTHENTICATION_CONTEXT");

        contexts.reset();
        state.revokeWorkload(CONSUMER);
        assertThat(send(canonical()).reasonCode()).isEqualTo("WORKLOAD_REVOKED");
        assertThat(sink.handoffCount()).isZero();
    }

    @Test
    // tag::protected-resource-checks-test[]
    void protectedAuthorityVersionsStateAndTargetAccountCanEachVetoHandoff() {
        assertDenied(admit(copy(canonical(), "workload:resolution-outbox-relay", TENANT,
                RESOURCE, "wrong-authority", 2, 1, null), "wrong-authority-17",
                "workload:resolution-outbox-relay", TENANT), "AUTHORITY_MISMATCH");
        assertDenied(admit(copy(canonical(), "workload:resolution-outbox-relay", TENANT,
                RESOURCE, DELEGATION, 1, 1, null), "wrong-version-17",
                "workload:resolution-outbox-relay", TENANT), "RESOURCE_VERSION_MISMATCH");

        var canonical = state.resource(RESOURCE).orElseThrow();
        state.putResource(resource(canonical, TERMINAL, canonical.targetTenantId(), false));
        assertDenied(canonical(), "RESOURCE_STATE_NOT_DISPATCHABLE");

        state.putResource(resource(canonical, READY_FOR_SECURITY_ADMISSION, "tenant-us", false));
        assertDenied(canonical(), "TARGET_ACCOUNT_TENANT_MISMATCH");

        state.putResource(resource(canonical, READY_FOR_SECURITY_ADMISSION, TENANT, true));
        assertDenied(canonical(), "AUTHORITY_REVOKED_OR_STALE");
        assertThat(sink.handoffCount()).isZero();
    }
    // end::protected-resource-checks-test[]

    @Test
    // tag::break-glass-scope-test[]
    void breakGlassReloadsAndValidatesIssuerPolicyAudienceScopeAndSecondActor() {
        BreakGlassGrant base = state.canonicalGrant(NOW);
        state.putGrant(base);
        contexts.useForTest(operatorContext("PHISHING_RESISTANT_MFA",
                Set.of("INCIDENT_COMMANDER")));

        SecuredRouteMessage copiedRelayClaim = readmission(
                "break-glass-17-01", "copied-relay-claim", "workload:diagnostic-replayer");
        assertDenied(copiedRelayClaim, "PRODUCER_PROVENANCE_MISMATCH");

        SecuredRouteMessage readmission = readmission("break-glass-17-01");

        SecurityDecision allowed = send(readmission);
        assertThat(allowed.outcome()).isEqualTo(ALLOW);
        assertThat(allowed.reasonCode()).isEqualTo("CURRENT_BREAK_GLASS_GRANT_PERMITS");
        assertThat(allowed.breakGlassGrantRef()).isEqualTo("break-glass-17-01");

        assertDenied(readmission("fabricated-grant"), "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, "service:attacker", base.policyRef(), base.audience(),
                base.serviceRef(), base.actions(), base.resourceRefs(), base.issuedAt(),
                false, false, base.requiredAssurance(), base.requiredRole(),
                base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, base.issuerRef(), "policy://order-desk/break-glass@2",
                base.audience(), base.serviceRef(), base.actions(), base.resourceRefs(),
                base.issuedAt(), false, false, base.requiredAssurance(),
                base.requiredRole(), base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, base.issuerRef(), base.policyRef(), "artifact-store",
                base.serviceRef(), base.actions(), base.resourceRefs(), base.issuedAt(),
                false, false, base.requiredAssurance(), base.requiredRole(),
                base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, base.issuerRef(), base.policyRef(), base.audience(),
                "service:other-gateway", base.actions(), base.resourceRefs(), base.issuedAt(),
                false, false, base.requiredAssurance(), base.requiredRole(),
                base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, base.issuerRef(), base.policyRef(), base.audience(),
                base.serviceRef(), Set.of(DISPATCH_COMPENSATION), base.resourceRefs(),
                base.issuedAt(), false, false, base.requiredAssurance(),
                base.requiredRole(), base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, base.issuerRef(), base.policyRef(), base.audience(),
                base.serviceRef(), base.actions(), Set.of(RESOURCE), base.issuedAt(),
                false, false, base.requiredAssurance(), base.requiredRole(),
                base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, base.issuerRef(), base.policyRef(), base.audience(),
                base.serviceRef(), base.actions(), base.resourceRefs(), NOW.plusSeconds(1),
                false, false, base.requiredAssurance(), base.requiredRole(),
                base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");

        state.putGrant(grant(base, base.issuerRef(), base.policyRef(), base.audience(),
                base.serviceRef(), base.actions(), base.resourceRefs(), base.issuedAt(),
                true, false, base.requiredAssurance(), base.requiredRole(),
                base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, base.issuerRef(), base.policyRef(), base.audience(),
                base.serviceRef(), base.actions(), base.resourceRefs(), base.issuedAt(),
                false, true, base.requiredAssurance(), base.requiredRole(),
                base.secondActorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(grant(base, base.issuerRef(), base.policyRef(), base.audience(),
                base.serviceRef(), base.actions(), base.resourceRefs(), base.issuedAt(),
                false, false, base.requiredAssurance(), base.requiredRole(), base.actorRef()));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        state.putGrant(withExpiry(base, NOW));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");

        state.putGrant(base);
        contexts.useForTest(operatorContext("PASSWORD", Set.of("INCIDENT_COMMANDER")));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        contexts.useForTest(operatorContext("PHISHING_RESISTANT_MFA", Set.of("VIEWER")));
        assertDenied(readmission, "LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
        assertThat(sink.handoffCount()).isOne();
    }
    // end::break-glass-scope-test[]

    @Test
    // tag::credential-redaction-test[]
    void everyCallerControlledStringIsAllowlistedBeforeAuditPersistence() {
        String secret = "Bearer-stolen-value";
        var bad = Set.of(
                new SecuredRouteMessage(secret,
                        "provenance://ingress/msg-compensation-dispatch-019501",
                        "workload:resolution-outbox-relay", TENANT, SUBJECT, DELEGATION,
                        DISPATCH_COMPENSATION, RESOURCE, DELEGATION, 2, 1, null),
                new SecuredRouteMessage("msg-compensation-dispatch-019501", secret,
                        "workload:resolution-outbox-relay", TENANT, SUBJECT, DELEGATION,
                        DISPATCH_COMPENSATION, RESOURCE, DELEGATION, 2, 1, null),
                copy(canonical(), secret, TENANT, RESOURCE, DELEGATION, 2, 1, null),
                copy(canonical(), "workload:resolution-outbox-relay", secret,
                        RESOURCE, DELEGATION, 2, 1, null),
                new SecuredRouteMessage("msg-compensation-dispatch-019501",
                        "provenance://ingress/msg-compensation-dispatch-019501",
                        "workload:resolution-outbox-relay", TENANT, secret, DELEGATION,
                        DISPATCH_COMPENSATION, RESOURCE, DELEGATION, 2, 1, null),
                new SecuredRouteMessage("msg-compensation-dispatch-019501",
                        "provenance://ingress/msg-compensation-dispatch-019501",
                        "workload:resolution-outbox-relay", TENANT, SUBJECT, secret,
                        DISPATCH_COMPENSATION, RESOURCE, DELEGATION, 2, 1, null),
                copy(canonical(), "workload:resolution-outbox-relay", TENANT,
                        secret, DELEGATION, 2, 1, null),
                copy(canonical(), "workload:resolution-outbox-relay", TENANT,
                        RESOURCE, secret, 2, 1, null),
                copy(canonical(), "workload:resolution-outbox-relay", TENANT,
                        RESOURCE, DELEGATION, 2, 1, secret),
                new SecuredRouteMessage(null,
                        "provenance://ingress/msg-compensation-dispatch-019501",
                        "workload:resolution-outbox-relay", TENANT, SUBJECT, DELEGATION,
                        DISPATCH_COMPENSATION, RESOURCE, DELEGATION, 2, 1, null),
                new SecuredRouteMessage("x".repeat(161),
                        "provenance://ingress/msg-compensation-dispatch-019501",
                        "workload:resolution-outbox-relay", TENANT, SUBJECT, DELEGATION,
                        DISPATCH_COMPENSATION, RESOURCE, DELEGATION, 2, 1, null));

        bad.forEach(message -> {
            SecurityDecision decision = send(message);
            assertThat(decision.reasonCode()).isEqualTo("INVALID_OR_UNSAFE_MESSAGE_FIELD");
            assertThat(decision.invalidFieldCode()).isNotBlank();
            assertThat(decision.messageSha256()).isNull();
        });
        String persisted = jdbc.queryForList("select * from route_security_decision").toString();
        assertThat(persisted).doesNotContain("stolen-value", sha256(secret));
        bad.stream().map(RouteMessageIntegrity::sha256)
                .forEach(digest -> assertThat(persisted).doesNotContain(digest));
        assertThat(sink.handoffCount()).isZero();
    }
    // end::credential-redaction-test[]

    @Test
    // tag::revoked-workload-containment-test[]
    void denialCreatesProtectedQuarantineAndASeparateFreshAdmissionCanProceed() {
        state.revokeWorkload(CONSUMER);
        SecurityDecision denied = send(canonical());

        assertThat(denied.reasonCode()).isEqualTo("WORKLOAD_REVOKED");
        assertThat(jdbc.queryForMap(
                "select * from route_security_quarantine where decision_id=?",
                denied.decisionId()))
                .containsEntry("QUARANTINE_STATE", "AWAITING_TYPED_REDRIVE_OR_DISPOSITION");
        assertThat(sink.handoffCount()).isZero();

        state.reset(NOW); // fixture equivalent of containment plus fresh transport admission
        SecurityDecision readmitted = send(canonical());
        assertThat(readmitted.outcome()).isEqualTo(ALLOW);
        assertThat(readmitted.decisionId()).isNotEqualTo(denied.decisionId());
        assertThat(readmitted.messageSha256()).isEqualTo(denied.messageSha256());
        assertThat(sink.handoffCount()).isOne();
    }
    // end::revoked-workload-containment-test[]

    @Test
    void requiredDecisionAuditFailureRollsBackDenialAndQuarantine() {
        state.revokeWorkload(CONSUMER);
        hook.fail = true;

        assertThatThrownBy(() -> send(canonical())).isInstanceOf(RuntimeException.class);
        assertThat(jdbc.queryForObject(
                "select count(*) from route_security_decision", Integer.class)).isZero();
        assertThat(jdbc.queryForObject(
                "select count(*) from route_security_quarantine", Integer.class)).isZero();
        assertThat(sink.handoffCount()).isZero();
    }

    private void assertDenied(SecuredRouteMessage message, String reason) {
        SecurityDecision decision = send(message);
        assertThat(decision.outcome()).isEqualTo(DENY);
        assertThat(decision.reasonCode()).isEqualTo(reason);
    }

    private SecurityDecision send(SecuredRouteMessage message) {
        return producer.requestBody("direct:route-security-pregate", message, SecurityDecision.class);
    }

    private SecuredRouteMessage canonical() {
        return state.canonicalMessage();
    }

    private SecuredRouteMessage readmission(String grantRef) {
        return readmission(grantRef, "readmission-" + grantRef, RELAY);
    }

    private SecuredRouteMessage readmission(
            String grantRef, String provenanceSuffix, String authenticatedPublisher) {
        SecuredRouteMessage message = new SecuredRouteMessage(
                "msg-break-glass-readmission-019502",
                "caller-supplied-provenance-is-ignored",
                RELAY, TENANT, SUBJECT, DELEGATION,
                READMIT_RETAINED_MESSAGE, EVENT_RESOURCE, DELEGATION, 2, 1, grantRef);
        return admit(message, provenanceSuffix, authenticatedPublisher, TENANT);
    }

    private SecuredRouteMessage admit(
            SecuredRouteMessage source, String provenanceSuffix,
            String authenticatedProducer, String producerTenant) {
        SecuredRouteMessage accepted = new SecuredRouteMessage(
                source.messageId(), "provenance://ingress/" + provenanceSuffix,
                source.producerClaimRef(), source.claimedTenantId(), source.subjectRef(),
                source.delegationAuthorityRef(), source.action(), source.resourceRef(),
                source.claimedAuthorityRef(), source.expectedPlanVersion(),
                source.expectedEffectVersion(), source.breakGlassGrantRef());
        state.putProvenance(new ProducerProvenance(
                accepted.producerProvenanceRef(), accepted.messageId(), authenticatedProducer,
                producerTenant, RouteMessageIntegrity.sha256(accepted),
                NOW.minusSeconds(5), NOW.plusSeconds(60), false));
        return accepted;
    }

    private SecuredRouteMessage copy(
            SecuredRouteMessage source, String producer, String tenant, String resource,
            String authority, long planVersion, long effectVersion, String grantRef) {
        return new SecuredRouteMessage(
                source.messageId(), source.producerProvenanceRef(), producer, tenant,
                source.subjectRef(), authority, source.action(), resource, authority,
                planVersion, effectVersion, grantRef);
    }

    private ProtectedRouteContext context(
            String workload, Set<String> tenants, String audience, String assurance,
            Set<String> roles, String actor, Instant expiresAt) {
        return new ProtectedRouteContext(
                workload, "service:order-desk-effect-gateway", actor, tenants, audience,
                "mTLS", "svid-key-41", roles, assurance,
                NOW.minusSeconds(10), expiresAt);
    }

    private ProtectedRouteContext operatorContext(String assurance, Set<String> roles) {
        return context("workload:on-call-operator", Set.of(TENANT),
                JdbcRouteSecurityGate.EFFECT_AUDIENCE, assurance, roles,
                "user:ops:347", NOW.plusSeconds(120));
    }

    private SecuredResourceSnapshot resource(
            SecuredResourceSnapshot current, SecuredResourceSnapshot.State next,
            String targetTenant, boolean authorityRevoked) {
        return new SecuredResourceSnapshot(
                current.resourceRef(), current.tenantId(), current.subjectRef(),
                current.delegationAuthorityRef(), current.authorityRef(), current.planId(),
                current.planVersion(), current.effectVersion(), next,
                "warehouse-account:" + targetTenant, targetTenant,
                current.authorityValidUntil(), authorityRevoked);
    }

    private BreakGlassGrant grant(
            BreakGlassGrant base, String issuer, String policy, String audience,
            String service, Set<RouteAction> actions, Set<String> resources,
            Instant issuedAt, boolean revoked, boolean superseded,
            String assurance, String role, String secondActor) {
        return new BreakGlassGrant(
                base.grantRef(), issuer, policy, audience, service, base.actorRef(),
                base.tenantId(), actions, resources, base.reasonCode(),
                issuedAt, base.expiresAt(), revoked, superseded, assurance, role, secondActor);
    }

    private BreakGlassGrant withExpiry(BreakGlassGrant base, Instant expiresAt) {
        return new BreakGlassGrant(
                base.grantRef(), base.issuerRef(), base.policyRef(), base.audience(),
                base.serviceRef(), base.actorRef(), base.tenantId(), base.actions(),
                base.resourceRefs(), base.reasonCode(), base.issuedAt(), expiresAt,
                base.revoked(), base.superseded(), base.requiredAssurance(),
                base.requiredRole(), base.secondActorRef());
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestBeans {
        @Bean
        @Primary
        Clock fixedClock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        @Primary
        FailingHook failingHook() {
            return new FailingHook();
        }
    }

    static final class FailingHook implements AfterSecurityDecisionHook {
        volatile boolean fail;

        @Override
        public void afterDecision(SecurityDecision decision) {
            if (fail) throw new IllegalStateException("forced audit sink failure");
        }
    }
}
