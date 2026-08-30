package dev.agenticintegrationpatterns.orderdesk.security;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static dev.agenticintegrationpatterns.orderdesk.security.RouteAction.READMIT_RETAINED_MESSAGE;
import static dev.agenticintegrationpatterns.orderdesk.security.SecuredResourceSnapshot.State.READY_FOR_SECURITY_ADMISSION;

/** Protected fixture state standing in for ingress, authority, resource, and grant repositories. */
@Component
public class ProtectedRouteState {
    public static final String TENANT = "tenant-ca";
    public static final String SUBJECT = "principal:order-ops-ca";
    public static final String DELEGATION = "recovery-authority-019498";
    public static final String RESOURCE = "effect://tenant-ca/effect-release-reserve-16";
    public static final String EVENT_RESOURCE = "event://tenant-ca/evt-019498";
    public static final String BREAK_GLASS_ISSUER = "service:incident-authority";
    public static final String BREAK_GLASS_POLICY = "policy://order-desk/break-glass@3";

    private final Map<String, ProducerProvenance> provenance = new ConcurrentHashMap<>();
    private final Map<String, SecuredResourceSnapshot> resources = new ConcurrentHashMap<>();
    private final Map<String, BreakGlassGrant> grants = new ConcurrentHashMap<>();
    private final Set<String> revokedWorkloads = ConcurrentHashMap.newKeySet();

    public void reset(Instant now) {
        provenance.clear();
        resources.clear();
        grants.clear();
        revokedWorkloads.clear();
        SecuredRouteMessage canonical = canonicalMessage();
        provenance.put(canonical.producerProvenanceRef(), new ProducerProvenance(
                canonical.producerProvenanceRef(), canonical.messageId(),
                canonical.producerClaimRef(), TENANT, RouteMessageIntegrity.sha256(canonical),
                now.minusSeconds(30), now.plusSeconds(300), false));
        resources.put(RESOURCE, resource(RESOURCE, TENANT, TENANT,
                READY_FOR_SECURITY_ADMISSION, 2, 1, false));
        resources.put("effect://tenant-us/effect-release-reserve-16", resource(
                "effect://tenant-us/effect-release-reserve-16", "tenant-us", "tenant-us",
                READY_FOR_SECURITY_ADMISSION, 2, 1, false));
        resources.put(EVENT_RESOURCE, resource(EVENT_RESOURCE, TENANT, TENANT,
                READY_FOR_SECURITY_ADMISSION, 2, 1, false));
    }

    private SecuredResourceSnapshot resource(
            String ref, String tenant, String targetTenant, SecuredResourceSnapshot.State state,
            long planVersion, long effectVersion, boolean authorityRevoked) {
        return new SecuredResourceSnapshot(
                ref, tenant, SUBJECT, DELEGATION, DELEGATION,
                "resolution-plan-019494", planVersion, effectVersion, state,
                "warehouse-account:" + targetTenant, targetTenant,
                Instant.parse("2026-08-27T14:10:00Z"), authorityRevoked);
    }

    public Optional<ProducerProvenance> provenance(String ref) {
        return Optional.ofNullable(provenance.get(ref));
    }

    public Optional<SecuredResourceSnapshot> resource(String ref) {
        return Optional.ofNullable(resources.get(ref));
    }

    public Optional<BreakGlassGrant> grant(String ref) {
        return Optional.ofNullable(grants.get(ref));
    }

    public boolean workloadRevoked(String workloadRef) {
        return revokedWorkloads.contains(workloadRef);
    }

    public void revokeWorkload(String workloadRef) {
        revokedWorkloads.add(workloadRef);
    }

    public void putResource(SecuredResourceSnapshot resource) {
        resources.put(resource.resourceRef(), resource);
    }

    public void putProvenance(ProducerProvenance value) {
        provenance.put(value.provenanceRef(), value);
    }

    public SecuredRouteMessage canonicalMessage() {
        return new SecuredRouteMessage(
                "msg-compensation-dispatch-019501",
                "provenance://ingress/msg-compensation-dispatch-019501",
                "workload:resolution-outbox-relay", TENANT, SUBJECT, DELEGATION,
                RouteAction.DISPATCH_COMPENSATION, RESOURCE, DELEGATION, 2, 1, null);
    }

    public void putGrant(BreakGlassGrant grant) {
        grants.put(grant.grantRef(), grant);
    }

    public BreakGlassGrant canonicalGrant(Instant now) {
        return new BreakGlassGrant(
                "break-glass-17-01", BREAK_GLASS_ISSUER, BREAK_GLASS_POLICY,
                JdbcRouteSecurityGate.EFFECT_AUDIENCE,
                "service:order-desk-effect-gateway", "user:ops:347", TENANT,
                Set.of(READMIT_RETAINED_MESSAGE), Set.of(EVENT_RESOURCE), "INCIDENT_8421",
                now.minusSeconds(5), now.plusSeconds(60), false, false,
                "PHISHING_RESISTANT_MFA", "INCIDENT_COMMANDER", "user:ops:512");
    }
}
