package dev.agenticintegrationpatterns.orderdesk.security;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import static dev.agenticintegrationpatterns.orderdesk.security.SecurityDecision.Outcome.ALLOW;
import static dev.agenticintegrationpatterns.orderdesk.security.SecurityDecision.Outcome.DENY;

/** A pre-gate before the existing authority/effect-state dispatch boundary. */
@Component
public class JdbcRouteSecurityGate {
    public static final String EFFECT_AUDIENCE = "order-desk-effect-gateway";
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._:@-]{1,160}");
    private static final Pattern REF = Pattern.compile("[a-z][a-z0-9-]{1,30}://[A-Za-z0-9._:@/-]{1,540}");
    private static final Pattern RESOURCE = Pattern.compile(
            "(?:effect|event)://[a-z0-9-]{1,64}/[A-Za-z0-9._:@-]{1,200}");
    private static final Pattern CREDENTIAL_LIKE = Pattern.compile(
            "(?i)^(?:bearer|basic|token|password|secret|api[_-]?key)(?:\\s|[:=_-]).*");

    private final JdbcTemplate jdbc;
    private final RouteSecurityPolicy policy;
    private final ProtectedRouteState state;
    private final Clock clock;
    private final AfterSecurityDecisionHook afterDecision;

    public JdbcRouteSecurityGate(
            JdbcTemplate jdbc, RouteSecurityPolicy policy, ProtectedRouteState state,
            Clock clock, AfterSecurityDecisionHook afterDecision) {
        this.jdbc = jdbc;
        this.policy = policy;
        this.state = state;
        this.clock = clock;
        this.afterDecision = afterDecision;
    }

    // tag::route-security-gate[]
    @Transactional
    public SecurityDecision authorize(SecurityAdmission admission) {
        Instant now = clock.instant();
        SecuredRouteMessage message = admission.message();
        ProtectedRouteContext context = admission.protectedContext();
        Evaluation evaluation = evaluate(message, context, now);
        SecurityDecision decision = decision(message, context, evaluation, now);
        jdbc.update("""
                insert into route_security_decision
                (decision_id, message_id, message_sha256, tenant_id,
                 authenticated_workload_ref, service_ref, actor_ref, action_name, resource_ref,
                 producer_provenance_ref, producer_claim_sha256, tenant_claim_sha256,
                 resource_claim_sha256, subject_claim_sha256, delegation_claim_sha256,
                 audience, policy_ref, policy_sha256, outcome, reason_code,
                 invalid_field_code, break_glass_grant_ref, decided_at, context_expires_at)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, decision.decisionId(), decision.messageId(), decision.messageSha256(),
                decision.tenantId(), decision.authenticatedWorkloadRef(), decision.serviceRef(),
                decision.actorRef(), decision.actionName(), decision.resourceRef(),
                decision.producerProvenanceRef(), decision.producerClaimSha256(),
                decision.tenantClaimSha256(), decision.resourceClaimSha256(),
                decision.subjectClaimSha256(), decision.delegationClaimSha256(),
                decision.audience(), decision.policyRef(), decision.policySha256(),
                decision.outcome().name(), decision.reasonCode(), decision.invalidFieldCode(),
                decision.breakGlassGrantRef(), decision.decidedAt(), decision.contextExpiresAt());
        if (decision.outcome() == DENY) {
            jdbc.update("""
                    insert into route_security_quarantine
                    (decision_id, tenant_id, message_sha256, reason_code, quarantine_state,
                     recorded_at) values (?,?,?,?,?,?)
                    """, decision.decisionId(), decision.tenantId(), decision.messageSha256(),
                    decision.reasonCode(), "AWAITING_TYPED_REDRIVE_OR_DISPOSITION", now);
        }
        afterDecision.afterDecision(decision);
        return decision;
    }

    private Evaluation evaluate(
            SecuredRouteMessage message, ProtectedRouteContext context, Instant now) {
        InvalidField invalidField = invalidField(message);
        if (invalidField != null) {
            return Evaluation.deny("INVALID_OR_UNSAFE_MESSAGE_FIELD", invalidField.code());
        }
        if (context == null) return Evaluation.deny("MISSING_PROTECTED_CONTEXT");
        if (!validContext(context)) return Evaluation.deny("INVALID_PROTECTED_CONTEXT");
        if (!Objects.equals(EFFECT_AUDIENCE, context.audience()))
            return Evaluation.deny("WRONG_AUDIENCE");
        if (context.expiresAt() == null || !now.isBefore(context.expiresAt()))
            return Evaluation.deny("STALE_AUTHENTICATION_CONTEXT");
        if (state.workloadRevoked(context.authenticatedWorkloadRef()))
            return Evaluation.deny("WORKLOAD_REVOKED");

        ProducerProvenance provenance = state.provenance(message.producerProvenanceRef())
                .orElse(null);
        if (provenance == null) return Evaluation.deny("UNVERIFIED_PRODUCER_PROVENANCE");
        if (provenance.revoked() || provenance.expiresAt() == null
                || !now.isBefore(provenance.expiresAt()))
            return Evaluation.deny("PRODUCER_PROVENANCE_REVOKED_OR_STALE");
        if (!Objects.equals(provenance.messageId(), message.messageId())
                || !Objects.equals(provenance.producerWorkloadRef(), message.producerClaimRef()))
            return Evaluation.deny("PRODUCER_PROVENANCE_MISMATCH");
        if (!matchesSha256(provenance.integritySha256())
                || !Objects.equals(provenance.integritySha256(),
                        RouteMessageIntegrity.sha256(message)))
            return Evaluation.deny("PRODUCER_RECORD_INTEGRITY_MISMATCH");

        SecuredResourceSnapshot resource = state.resource(message.resourceRef()).orElse(null);
        if (resource == null) return Evaluation.deny("RESOURCE_NOT_FOUND");
        if (!context.permittedTenantIds().contains(resource.tenantId())
                || !Objects.equals(provenance.tenantId(), resource.tenantId())
                || !Objects.equals(message.claimedTenantId(), resource.tenantId()))
            return Evaluation.deny("TENANT_BOUNDARY_MISMATCH");
        if (!Objects.equals(resource.targetTenantId(), resource.tenantId())
                || !Objects.equals(resource.targetAccountRef(),
                        "warehouse-account:" + resource.tenantId()))
            return Evaluation.deny("TARGET_ACCOUNT_TENANT_MISMATCH");
        if (!Objects.equals(message.subjectRef(), resource.subjectRef()))
            return Evaluation.deny("SUBJECT_MISMATCH");
        if (!Objects.equals(message.delegationAuthorityRef(), resource.delegationAuthorityRef())
                || !Objects.equals(message.claimedAuthorityRef(), resource.authorityRef()))
            return Evaluation.deny("AUTHORITY_MISMATCH");
        if (message.expectedPlanVersion() != resource.planVersion()
                || message.expectedEffectVersion() != resource.effectVersion())
            return Evaluation.deny("RESOURCE_VERSION_MISMATCH");
        if (resource.state() != SecuredResourceSnapshot.State.READY_FOR_SECURITY_ADMISSION)
            return Evaluation.deny("RESOURCE_STATE_NOT_DISPATCHABLE");
        if (resource.authorityRevoked() || !now.isBefore(resource.authorityValidUntil()))
            return Evaluation.deny("AUTHORITY_REVOKED_OR_STALE");

        if (policy.permits(context.authenticatedWorkloadRef(), context.serviceRef(),
                resource.tenantId(), message.action(), resource.targetAccountRef())) {
            return Evaluation.allow("CURRENT_ROUTE_POLICY_PERMITS", null, resource.tenantId());
        }
        BreakGlassGrant grant = message.breakGlassGrantRef() == null
                ? null : state.grant(message.breakGlassGrantRef()).orElse(null);
        return validBreakGlass(message, context, resource, grant, now)
                ? Evaluation.allow("CURRENT_BREAK_GLASS_GRANT_PERMITS", grant.grantRef(),
                        resource.tenantId())
                : Evaluation.deny("LEAST_PRIVILEGE_OR_BREAK_GLASS_DENIAL");
    }

    private boolean validBreakGlass(
            SecuredRouteMessage message, ProtectedRouteContext context,
            SecuredResourceSnapshot resource, BreakGlassGrant grant, Instant now) {
        return grant != null
                && !grant.revoked() && !grant.superseded()
                && grant.issuedAt() != null && grant.expiresAt() != null
                && !now.isBefore(grant.issuedAt()) && now.isBefore(grant.expiresAt())
                && Objects.equals(ProtectedRouteState.BREAK_GLASS_ISSUER, grant.issuerRef())
                && Objects.equals(ProtectedRouteState.BREAK_GLASS_POLICY, grant.policyRef())
                && Objects.equals(EFFECT_AUDIENCE, grant.audience())
                && Objects.equals(context.serviceRef(), grant.serviceRef())
                && context.actorRef() != null
                && Objects.equals(grant.actorRef(), context.actorRef())
                && Objects.equals(grant.tenantId(), resource.tenantId())
                && grant.actions().contains(message.action())
                && grant.resourceRefs().contains(resource.resourceRef())
                && grant.reasonCode() != null && !grant.reasonCode().isBlank()
                && Objects.equals(grant.requiredAssurance(), context.assuranceLevel())
                && context.roles().contains(grant.requiredRole())
                && grant.secondActorRef() != null
                && !grant.secondActorRef().equals(grant.actorRef());
    }
    // end::route-security-gate[]

    private SecurityDecision decision(
            SecuredRouteMessage message, ProtectedRouteContext context,
            Evaluation evaluation, Instant now) {
        String tenant = evaluation.tenantId() != null ? evaluation.tenantId()
                : firstTenant(context, safeId(message == null ? null : message.claimedTenantId()));
        return new SecurityDecision(
                "security-decision-" + UUID.randomUUID(),
                safeId(message == null ? null : message.messageId()),
                invalidField(message) == null ? RouteMessageIntegrity.sha256(message) : null, tenant,
                context == null ? "unresolved" : safeId(context.authenticatedWorkloadRef()),
                context == null ? "unresolved" : safeId(context.serviceRef()),
                context == null || context.actorRef() == null ? null : safeId(context.actorRef()),
                message == null || message.action() == null ? "UNRESOLVED" : message.action().name(),
                safeResource(message == null ? null : message.resourceRef()),
                safeRef(message == null ? null : message.producerProvenanceRef()),
                safeClaimSha(ID, message == null ? null : message.producerClaimRef()),
                safeClaimSha(ID, message == null ? null : message.claimedTenantId()),
                safeClaimSha(RESOURCE, message == null ? null : message.resourceRef()),
                safeClaimSha(ID, message == null ? null : message.subjectRef()),
                safeClaimSha(ID, message == null ? null : message.delegationAuthorityRef()),
                context == null ? "unresolved" : safeId(context.audience()),
                RouteSecurityPolicy.POLICY_REF, RouteSecurityPolicy.POLICY_SHA256,
                evaluation.outcome(), evaluation.reasonCode(), evaluation.invalidFieldCode(),
                evaluation.usedGrantRef(), now,
                context == null || context.expiresAt() == null ? now : context.expiresAt());
    }

    private static InvalidField invalidField(SecuredRouteMessage m) {
        if (m == null) return new InvalidField("MESSAGE:NULL");
        if (m.action() == null) return new InvalidField("ACTION:NULL");
        InvalidField invalid = invalid("MESSAGE_ID", ID, m.messageId());
        if (invalid != null) return invalid;
        invalid = invalid("PRODUCER_PROVENANCE_REF", REF, m.producerProvenanceRef());
        if (invalid != null) return invalid;
        invalid = invalid("PRODUCER_CLAIM_REF", ID, m.producerClaimRef());
        if (invalid != null) return invalid;
        invalid = invalid("CLAIMED_TENANT_ID", ID, m.claimedTenantId());
        if (invalid != null) return invalid;
        invalid = invalid("SUBJECT_REF", ID, m.subjectRef());
        if (invalid != null) return invalid;
        invalid = invalid("DELEGATION_AUTHORITY_REF", ID, m.delegationAuthorityRef());
        if (invalid != null) return invalid;
        invalid = invalid("RESOURCE_REF", RESOURCE, m.resourceRef());
        if (invalid != null) return invalid;
        invalid = invalid("CLAIMED_AUTHORITY_REF", ID, m.claimedAuthorityRef());
        if (invalid != null) return invalid;
        return m.breakGlassGrantRef() == null
                ? null : invalid("BREAK_GLASS_GRANT_REF", ID, m.breakGlassGrantRef());
    }

    private static InvalidField invalid(String field, Pattern pattern, String value) {
        if (value == null) return new InvalidField(field + ":NULL");
        if (CREDENTIAL_LIKE.matcher(value).matches()) {
            return new InvalidField(field + ":CREDENTIAL_LIKE");
        }
        return pattern.matcher(value).matches()
                ? null : new InvalidField(field + ":FORMAT_OR_LENGTH");
    }

    private static boolean validContext(ProtectedRouteContext c) {
        return matches(ID, c.authenticatedWorkloadRef()) && matches(ID, c.serviceRef())
                && matches(ID, c.audience()) && matches(ID, c.authenticationMethod())
                && matches(ID, c.credentialKeyId()) && matches(ID, c.assuranceLevel())
                && c.permittedTenantIds().stream().allMatch(v -> matches(ID, v));
    }

    private static boolean matches(Pattern pattern, String value) {
        return value != null && !CREDENTIAL_LIKE.matcher(value).matches()
                && pattern.matcher(value).matches();
    }

    private static String safeId(String value) {
        return matches(ID, value) ? value : "[INVALID]";
    }

    private static String safeRef(String value) {
        return matches(REF, value) ? value : "[INVALID]";
    }

    private static String safeResource(String value) {
        return matches(RESOURCE, value) ? value : "[INVALID]";
    }

    private static String firstTenant(ProtectedRouteContext context, String fallback) {
        if (context == null || context.permittedTenantIds().isEmpty()) return fallback;
        return context.permittedTenantIds().stream().map(JdbcRouteSecurityGate::safeId)
                .sorted().findFirst().orElse(fallback);
    }

    private static String sha(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String safeClaimSha(Pattern pattern, String value) {
        return matches(pattern, value) ? sha(value) : null;
    }

    private static boolean matchesSha256(String value) {
        return value != null && value.matches("[0-9a-f]{64}");
    }

    private record Evaluation(
            SecurityDecision.Outcome outcome, String reasonCode,
            String invalidFieldCode, String usedGrantRef, String tenantId) {
        static Evaluation deny(String reason) {
            return new Evaluation(DENY, reason, null, null, null);
        }
        static Evaluation deny(String reason, String invalidFieldCode) {
            return new Evaluation(DENY, reason, invalidFieldCode, null, null);
        }
        static Evaluation allow(String reason, String grant, String tenant) {
            return new Evaluation(ALLOW, reason, null, grant, tenant);
        }
    }

    private record InvalidField(String code) {
    }
}
