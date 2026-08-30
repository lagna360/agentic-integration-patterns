package dev.agenticintegrationpatterns.orderdesk.approval;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static dev.agenticintegrationpatterns.orderdesk.approval.ApprovalDecision.Action.*;
import static dev.agenticintegrationpatterns.orderdesk.approval.ApprovalReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.approval.ApprovalReceipt.State.*;

/** Application-specific durable approval aggregate, not a workflow or authorization protocol. */
@Component
public class JdbcApprovalService {
    private final JdbcTemplate jdbc;
    private final ApprovalPolicy policy;
    private final Clock clock;
    private final AfterApprovalStateHook afterState;

    public JdbcApprovalService(
            JdbcTemplate jdbc, ApprovalPolicy policy, Clock clock,
            AfterApprovalStateHook afterState) {
        this.jdbc = jdbc;
        this.policy = policy;
        this.clock = clock;
        this.afterState = afterState;
    }

    // tag::approval-policy-and-wait[]
    @Transactional
    public ApprovalReceipt open(String requestId, ApprovalSubject subject) {
        ApprovalSubject.require(requestId, "requestId", 160);
        String subjectSha = ApprovalFingerprints.subject(subject);
        var existing = find(subject.tenantId(), requestId);
        if (existing.isPresent()) {
            return existing.get().subjectSha256().equals(subjectSha)
                    ? receipt(DUPLICATE_SAME, existing.get())
                    : receipt(IDENTITY_COLLISION, existing.get());
        }

        Instant now = clock.instant();
        var decision = policy.evaluate(subject);
        var state = initialState(decision.disposition(), now, subject.evidenceValidUntil());
        Instant dueAt = earlier(now.plus(decision.decisionWindow()), subject.evidenceValidUntil());
        Instant validUntil = earlier(now.plus(decision.authorityWindow()), subject.evidenceValidUntil());
        jdbc.update("""
                insert into approval_request
                (tenant_id, request_id, run_id, case_id, proposal_event_id, proposal_id,
                 effect_id, subject_sha256, subject_payload, proposer_ref,
                 policy_decision_id, policy_ref, policy_disposition, required_role, required_approvals,
                 approval_state, decision_due_at, authority_valid_until, version,
                 created_at, updated_at)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, subject.tenantId(), requestId, subject.runId(), subject.caseId(),
                subject.proposalEventId(), subject.proposalId(), subject.effectId(), subjectSha,
                ApprovalFingerprints.payload(subject), subject.proposerRef(), decision.decisionId(),
                decision.policyRef(), decision.disposition().name(), decision.requiredRole(),
                decision.requiredApprovals(),
                state.name(), dueAt, validUntil, 0L, now, now);
        afterState.afterStateMutation(subject.tenantId(), requestId, state, 0);
        insertOutbox(subject.tenantId(), requestId, subject.caseId(), 0,
                eventFor(state), Map.of("reason", decision.reasonCode()), now);
        return receipt(CREATED, find(subject.tenantId(), requestId).orElseThrow());
    }
    // end::approval-policy-and-wait[]

    // tag::trusted-approval-transition[]
    // tag::approval-decision-transaction[]
    @Transactional
    public ApprovalReceipt decide(ApprovalDecision decision, TrustedApproverContext actor) {
        String fingerprint = ApprovalFingerprints.decision(decision, actor);
        // Serialize decisions on one aggregate before consulting or writing the inbox.
        var request = findForUpdate(actor.tenantId(), decision.requestId());
        var duplicate = duplicate(actor.tenantId(), decision.decisionId(), fingerprint);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }
        if (request.isEmpty()) {
            insertInbox(actor.tenantId(), decision, fingerprint, DENIED.name());
            return empty(DENIED, actor.tenantId(), decision.requestId());
        }
        var current = request.get();
        if (decision.expectedVersion() != current.version()) {
            insertInbox(actor.tenantId(), decision, fingerprint, STALE_VERSION.name());
            return receipt(STALE_VERSION, current);
        }
        if (!permitted(decision.action(), actor, current.requiredRole())
                || actor.actorRef().equals(current.proposerRef())) {
            insertInbox(actor.tenantId(), decision, fingerprint, DENIED.name());
            return receipt(DENIED, current);
        }
        if (!transitionable(current.state(), decision.action())) {
            insertInbox(actor.tenantId(), decision, fingerprint, LATE.name());
            return receipt(LATE, current);
        }
        Instant now = clock.instant();
        if ((current.state() == PENDING || current.state() == ESCALATED)
                && !now.isBefore(current.decisionDueAt())) {
            var expired = transition(current, ApprovalReceipt.State.EXPIRED, now,
                    "ApprovalExpired", "DECISION_DUE_EXPIRED");
            insertInbox(actor.tenantId(), decision, fingerprint,
                    ApprovalReceipt.Disposition.EXPIRED.name());
            return expired;
        }

        int approvals = decision.action() == APPROVE
                ? approvalCount(current.tenantId(), current.requestId())
                    + (hasApproved(current.tenantId(), current.requestId(), actor.actorRef()) ? 0 : 1)
                : 0;
        var next = nextState(current, decision.action(), approvals);
        long nextVersion = current.version() + 1;
        int updated = jdbc.update("""
                update approval_request
                   set approval_state=?, version=?, updated_at=?
                 where tenant_id=? and request_id=? and version=? and approval_state=?
                """, next.name(), nextVersion, now, current.tenantId(), current.requestId(),
                current.version(), current.state().name());
        if (updated != 1) {
            insertInbox(actor.tenantId(), decision, fingerprint, STALE_VERSION.name());
            return receipt(STALE_VERSION,
                    find(actor.tenantId(), decision.requestId()).orElse(current));
        }
        jdbc.update("""
                insert into approval_decision
                (tenant_id, decision_id, request_id, actor_ref, action, reason_code,
                 request_version, decided_at) values (?,?,?,?,?,?,?,?)
                """, actor.tenantId(), decision.decisionId(), decision.requestId(), actor.actorRef(),
                decision.action().name(), decision.reasonCode(), current.version(), now);
        insertInbox(actor.tenantId(), decision, fingerprint, APPLIED.name());
        afterState.afterStateMutation(actor.tenantId(), decision.requestId(), next, nextVersion);
        String eventType = decision.action() == APPROVE && next == current.state()
                ? "ApprovalContributionRecorded" : eventFor(next);
        insertOutbox(actor.tenantId(), decision.requestId(), current.caseId(), nextVersion,
                eventType, Map.of("decisionId", decision.decisionId(),
                        "actorRef", actor.actorRef(), "action", decision.action().name()), now);
        return receipt(APPLIED, find(actor.tenantId(), decision.requestId()).orElseThrow());
    }
    // end::approval-decision-transaction[]
    // end::trusted-approval-transition[]

    @Transactional
    public ApprovalReceipt expire(String tenantId, String requestId, long expectedVersion) {
        var current = find(tenantId, requestId).orElseThrow();
        if (current.version() != expectedVersion) {
            return receipt(STALE_VERSION, current);
        }
        if (!(current.state() == PENDING || current.state() == ESCALATED
                || current.state() == APPROVED || current.state() == AUTO_AUTHORIZED)) {
            return receipt(LATE, current);
        }
        Instant now = clock.instant();
        boolean decisionWindow = current.state() == PENDING || current.state() == ESCALATED;
        Instant boundary = decisionWindow ? current.decisionDueAt() : current.authorityValidUntil();
        if (now.isBefore(boundary)) {
            return receipt(NOT_DUE, current);
        }
        return transition(current, ApprovalReceipt.State.EXPIRED, now,
                "ApprovalExpired", decisionWindow ? "DECISION_DUE_EXPIRED" : "AUTHORITY_EXPIRED");
    }

    // tag::current-authority-check[]
    @Transactional(readOnly = true)
    public ApprovalReceipt requireCurrentAuthority(
            String tenantId, String requestId, ApprovalSubject subject) {
        var row = find(tenantId, requestId)
                .orElseThrow(() -> new EffectAuthorityDeniedException("approval request is missing"));
        if (!row.subjectSha256().equals(ApprovalFingerprints.subject(subject))
                || !row.effectId().equals(subject.effectId())) {
            throw new EffectAuthorityDeniedException("approval subject does not match the effect");
        }
        if (row.state() != APPROVED && row.state() != AUTO_AUTHORIZED) {
            throw new EffectAuthorityDeniedException("approval authority is not active");
        }
        if (!clock.instant().isBefore(row.authorityValidUntil())) {
            throw new EffectAuthorityDeniedException("approval authority has expired");
        }
        return receipt(APPLIED, row);
    }

    @Transactional(readOnly = true)
    public ApprovalReceipt requireCurrentAuthority(
            String tenantId, String requestId, String effectId,
            String subjectSha256, long approvalVersion) {
        var row = find(tenantId, requestId)
                .orElseThrow(() -> new EffectAuthorityDeniedException("approval request is missing"));
        if (!row.effectId().equals(effectId)
                || !row.subjectSha256().equals(subjectSha256)
                || row.version() != approvalVersion) {
            throw new EffectAuthorityDeniedException("recorded authority does not match the effect");
        }
        if (row.state() != APPROVED && row.state() != AUTO_AUTHORIZED) {
            throw new EffectAuthorityDeniedException("approval authority is not active");
        }
        if (!clock.instant().isBefore(row.authorityValidUntil())) {
            throw new EffectAuthorityDeniedException("approval authority has expired");
        }
        return receipt(APPLIED, row);
    }
    // end::current-authority-check[]

    public ApprovalReceipt current(String tenantId, String requestId) {
        return receipt(APPLIED, find(tenantId, requestId).orElseThrow());
    }

    private ApprovalReceipt transition(
            ApprovalRow current, ApprovalReceipt.State next, Instant now,
            String eventType, String reason) {
        long nextVersion = current.version() + 1;
        int updated = jdbc.update("""
                update approval_request set approval_state=?, version=?, updated_at=?
                 where tenant_id=? and request_id=? and version=? and approval_state=?
                """, next.name(), nextVersion, now, current.tenantId(), current.requestId(),
                current.version(), current.state().name());
        if (updated != 1) {
            return receipt(STALE_VERSION, find(current.tenantId(), current.requestId()).orElseThrow());
        }
        afterState.afterStateMutation(current.tenantId(), current.requestId(), next, nextVersion);
        insertOutbox(current.tenantId(), current.requestId(), current.caseId(), nextVersion,
                eventType, Map.of("reason", reason), now);
        return receipt(APPLIED, find(current.tenantId(), current.requestId()).orElseThrow());
    }

    private Optional<ApprovalReceipt> duplicate(String tenantId, String id, String fingerprint) {
        var found = jdbc.query("""
                select request_id, payload_fingerprint from approval_message_inbox
                 where tenant_id=? and decision_id=?
                """, (rs, n) -> Map.entry(rs.getString(1), rs.getString(2)), tenantId, id);
        if (found.isEmpty()) return Optional.empty();
        var current = find(tenantId, found.get(0).getKey());
        if (found.get(0).getValue().equals(fingerprint)) {
            return Optional.of(current.map(r -> receipt(DUPLICATE_SAME, r))
                    .orElse(empty(DUPLICATE_SAME, tenantId, found.get(0).getKey())));
        }
        jdbc.update("""
                insert into approval_identity_collision
                (tenant_id, decision_id, existing_sha256, received_sha256, observed_at)
                values (?,?,?,?,?)
                """, tenantId, id, found.get(0).getValue(), fingerprint, clock.instant());
        return Optional.of(current.map(r -> receipt(IDENTITY_COLLISION, r))
                .orElse(empty(IDENTITY_COLLISION, tenantId, found.get(0).getKey())));
    }

    private void insertInbox(
            String tenantId, ApprovalDecision decision, String fingerprint, String disposition) {
        jdbc.update("""
                insert into approval_message_inbox
                (tenant_id, decision_id, request_id, payload_fingerprint, disposition, received_at)
                values (?,?,?,?,?,?)
                """, tenantId, decision.decisionId(), decision.requestId(), fingerprint,
                disposition, clock.instant());
    }

    private int approvalCount(String tenantId, String requestId) {
        return jdbc.queryForObject("""
                select count(distinct actor_ref) from approval_decision
                 where tenant_id=? and request_id=? and action='APPROVE'
                """, Integer.class, tenantId, requestId);
    }

    private boolean hasApproved(String tenantId, String requestId, String actorRef) {
        return jdbc.queryForObject("""
                select count(*) from approval_decision
                 where tenant_id=? and request_id=? and actor_ref=? and action='APPROVE'
                """, Integer.class, tenantId, requestId, actorRef) > 0;
    }

    private static boolean permitted(
            ApprovalDecision.Action action, TrustedApproverContext actor, String requiredRole) {
        return switch (action) {
            case SUPERSEDE -> actor.hasRole("ORDER_POLICY_ADMIN");
            case REVOKE -> actor.hasRole("ORDER_APPROVAL_REVOKER");
            default -> actor.hasRole(requiredRole);
        };
    }

    private static boolean transitionable(ApprovalReceipt.State state, ApprovalDecision.Action action) {
        if (action == REVOKE) return state == APPROVED;
        if (action == SUPERSEDE) return state == PENDING || state == ESCALATED || state == APPROVED;
        return state == PENDING || state == ESCALATED;
    }

    private static ApprovalReceipt.State nextState(
            ApprovalRow current, ApprovalDecision.Action action, int approvals) {
        return switch (action) {
            case APPROVE -> approvals >= current.requiredApprovals()
                    ? APPROVED : current.state();
            case REJECT -> REJECTED;
            case REQUEST_CHANGE -> CHANGES_REQUESTED;
            case ESCALATE -> ESCALATED;
            case REVOKE -> REVOKED;
            case SUPERSEDE -> SUPERSEDED;
        };
    }

    private static ApprovalReceipt.State initialState(
            ApprovalPolicyDecision.Disposition disposition, Instant now, Instant evidenceUntil) {
        if (!now.isBefore(evidenceUntil)) return INDETERMINATE;
        return switch (disposition) {
            case FORBIDDEN -> FORBIDDEN;
            case AUTO_AUTHORIZED -> AUTO_AUTHORIZED;
            case HUMAN_REQUIRED -> PENDING;
            case INDETERMINATE -> INDETERMINATE;
        };
    }

    private Optional<ApprovalRow> find(String tenantId, String requestId) {
        return queryApprovalRow("""
                select tenant_id, request_id, case_id, effect_id, subject_sha256, proposer_ref,
                       approval_state, required_role, required_approvals, decision_due_at,
                       authority_valid_until, version
                  from approval_request where tenant_id=? and request_id=?
                """, tenantId, requestId);
    }

    private Optional<ApprovalRow> findForUpdate(String tenantId, String requestId) {
        return queryApprovalRow("""
                select tenant_id, request_id, case_id, effect_id, subject_sha256, proposer_ref,
                       approval_state, required_role, required_approvals, decision_due_at,
                       authority_valid_until, version
                  from approval_request where tenant_id=? and request_id=? for update
                """, tenantId, requestId);
    }

    private Optional<ApprovalRow> queryApprovalRow(
            String sql, String tenantId, String requestId) {
        return jdbc.query(sql, (rs, n) -> new ApprovalRow(
                rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                rs.getString(5), rs.getString(6), ApprovalReceipt.State.valueOf(rs.getString(7)),
                rs.getString(8), rs.getInt(9), rs.getTimestamp(10).toInstant(),
                rs.getTimestamp(11).toInstant(), rs.getLong(12)), tenantId, requestId)
                .stream().findFirst();
    }

    private void insertOutbox(
            String tenantId, String requestId, String caseId, long version,
            String eventType, Map<String, ?> payload, Instant now) {
        jdbc.update("""
                insert into approval_outbox
                (event_id, tenant_id, request_id, case_id, approval_version,
                 event_type, event_payload, created_at)
                values (?,?,?,?,?,?,?,?)
                """, requestId + ":v" + version + ":" + eventType, tenantId, requestId,
                caseId, version, eventType, payload.toString(), now);
    }

    private static String eventFor(ApprovalReceipt.State state) {
        return switch (state) {
            case PENDING -> "ApprovalRequested";
            case AUTO_AUTHORIZED -> "EffectAutoAuthorized";
            case FORBIDDEN -> "EffectForbidden";
            case INDETERMINATE -> "PolicyEvaluationIndeterminate";
            case APPROVED -> "EffectApproved";
            case ESCALATED -> "ApprovalEscalated";
            case REJECTED -> "ApprovalRejected";
            case CHANGES_REQUESTED -> "ApprovalChangesRequested";
            case EXPIRED -> "ApprovalExpired";
            case REVOKED -> "ApprovalRevoked";
            case SUPERSEDED -> "ApprovalSuperseded";
        };
    }

    private static Instant earlier(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    private static ApprovalReceipt receipt(ApprovalReceipt.Disposition disposition, ApprovalRow row) {
        String authority = (row.state() == APPROVED || row.state() == AUTO_AUTHORIZED)
                ? "approval://" + row.tenantId() + "/" + row.requestId() + "@v" + row.version()
                : null;
        return new ApprovalReceipt(disposition, row.tenantId(), row.requestId(), row.state(),
                row.version(), row.subjectSha256(), row.authorityValidUntil(), authority);
    }

    private static ApprovalReceipt empty(
            ApprovalReceipt.Disposition disposition, String tenantId, String requestId) {
        return new ApprovalReceipt(disposition, tenantId, requestId, null, -1, null, null, null);
    }

    private record ApprovalRow(
            String tenantId, String requestId, String caseId, String effectId,
            String subjectSha256, String proposerRef, ApprovalReceipt.State state,
            String requiredRole, int requiredApprovals, Instant decisionDueAt, Instant authorityValidUntil,
            long version) {
    }
}
