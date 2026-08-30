package dev.agenticintegrationpatterns.orderdesk.peer;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static dev.agenticintegrationpatterns.orderdesk.peer.EstablishedRemotePeerSelectionPolicy.sha256;

/** Local ownership and containment for one independently operated, evidence-only peer. */
@Component
public class JdbcRemotePeerTaskManager {
    public static final String AUDIENCE = "order-desk-remote-evidence";
    public static final String SERVICE = "service:order-desk-peer-gateway";

    private final JdbcTemplate jdbc;
    private final EstablishedRemotePeerSelectionPolicy selection;
    private final Clock clock;
    private final AfterRemotePeerTaskReadHook afterRead;

    public JdbcRemotePeerTaskManager(
            JdbcTemplate jdbc, EstablishedRemotePeerSelectionPolicy selection, Clock clock,
            AfterRemotePeerTaskReadHook afterRead) {
        this.jdbc = jdbc;
        this.selection = selection;
        this.clock = clock;
        this.afterRead = afterRead;
    }

    // tag::open-remote-task-transaction[]
    @Transactional
    public RemoteTaskReceipt open(RemoteTaskDefinition task) {
        requireTask(task);
        NegotiatedPeerContract contract = selection.negotiate(task);
        Instant now = clock.instant();
        if (!now.isBefore(task.deadlineAt()))
            throw new IllegalArgumentException("LOCAL_DEADLINE_EXPIRED");

        jdbc.update("""
                insert into remote_peer_task
                (tenant_id, remote_work_id, case_id, correlation_id, local_owner_ref,
                 peer_ref, capability, protocol_family, protocol_version, interaction_profile,
                fixed_adapter_ref, registration_revision, advertisement_sha256,
                 objective, required_deliverables, input_artifact_ref, input_artifact_sha256,
                 result_schema_ref, deadline_at,
                 max_reported_tokens, max_reported_cost_micros, max_artifact_bytes,
                 reported_tokens, reported_cost_micros, task_state, version, created_at, updated_at)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, task.tenantId(), task.remoteWorkId(), task.caseId(), task.correlationId(),
                task.localOwnerRef(), contract.peerRef(), contract.capability(),
                contract.protocolFamily(), contract.protocolVersion(),
                contract.interactionProfile(), contract.fixedAdapterRef(),
                contract.registrationRevision(), contract.advertisementSha256(),
                task.objective(), canonicalSet(task.requiredDeliverables()),
                task.inputArtifactRef(), task.inputArtifactSha256(), task.resultSchemaRef(),
                task.deadlineAt(),
                task.maxReportedTokens(), task.maxReportedCostMicros(), task.maxArtifactBytes(),
                0L, 0L, "DISPATCH_PENDING", 1L, now, now);
        publish(task.tenantId(), task.remoteWorkId(), 1,
                "RemotePeerWorkRequested", "peer=" + contract.peerRef(), now);
        return receipt(task.tenantId(), task.remoteWorkId(), "OPENED", "CONTRACT_PINNED");
    }
    // end::open-remote-task-transaction[]

    // tag::accept-peer-update-transaction[]
    @Transactional
    public RemoteTaskReceipt accept(
            ProtectedRemotePeerContext context, RemotePeerUpdate update) {
        Instant now = clock.instant();
        String payloadSha = updateSha256(update);
        List<InboxRow> existing = jdbc.query("""
                select payload_sha256, remote_work_id, authenticated_peer_ref
                from remote_peer_message_inbox
                where tenant_id=? and message_id=?
                """, (rs, n) -> new InboxRow(rs.getString("payload_sha256"),
                        rs.getString("remote_work_id"),
                        rs.getString("authenticated_peer_ref")),
                update.tenantId(), update.messageId());
        if (!existing.isEmpty()) {
            InboxRow original = existing.get(0);
            TaskRow originalTask = load(update.tenantId(), original.remoteWorkId());
            String duplicateDenial = contextDenial(context, originalTask, update, now);
            if (duplicateDenial != null
                    || !Objects.equals(context.authenticatedPeerRef(),
                    original.authenticatedPeerRef()))
                return receipt(update.tenantId(), original.remoteWorkId(),
                        "DENIED", duplicateDenial == null
                                ? "ORIGINAL_MESSAGE_PEER_MISMATCH" : duplicateDenial);
            if (Objects.equals(original.payloadSha256(), payloadSha))
                return receipt(update.tenantId(), original.remoteWorkId(),
                        "DUPLICATE", "SAME_MESSAGE_SAME_CONTENT");
            if (isTerminal(originalTask.state())) {
                publish(originalTask.tenantId(), originalTask.remoteWorkId(),
                        originalTask.version(), "RemotePeerMessageCollisionDetected",
                        collisionEvidence(original.payloadSha256(), payloadSha), now);
                return receipt(update.tenantId(), original.remoteWorkId(),
                        "COLLISION_RECORDED", "TERMINAL_WORK_STATE_PRESERVED");
            }
            contain(originalTask, "MESSAGE_ID_COLLISION", now);
            publish(originalTask.tenantId(), originalTask.remoteWorkId(),
                    originalTask.version() + 1, "RemotePeerMessageCollisionDetected",
                    collisionEvidence(original.payloadSha256(), payloadSha), now);
            return receipt(update.tenantId(), original.remoteWorkId(),
                    "CONTAINED", "MESSAGE_ID_CHANGED_CONTENT");
        }

        TaskRow task = load(update.tenantId(), update.remoteWorkId());
        String denial = contextDenial(context, task, update, now);
        if (denial != null) {
            inbox(update, payloadSha, context == null ? "[MISSING]"
                    : context.authenticatedPeerRef(), "DENIED_" + denial, now);
            return receipt(update.tenantId(), update.remoteWorkId(), "DENIED", denial);
        }
        if (!now.isBefore(task.deadlineAt()) || "TIMED_OUT".equals(task.state())) {
            inbox(update, payloadSha, context.authenticatedPeerRef(), "LATE", now);
            return receipt(update.tenantId(), update.remoteWorkId(),
                    "LATE", "LOCAL_WORK_REMAINS_CLOSED");
        }
        if (isTerminal(task.state())) {
            inbox(update, payloadSha, context.authenticatedPeerRef(), "IGNORED_TERMINAL", now);
            return receipt(update.tenantId(), update.remoteWorkId(),
                    "IGNORED", "LOCAL_WORK_ALREADY_TERMINAL");
        }
        if (!task.protocolFamily().equals(update.protocolFamily())
                || !task.protocolVersion().equals(update.protocolVersion())
                || !task.interactionProfile().equals(update.interactionProfile())) {
            inbox(update, payloadSha, context.authenticatedPeerRef(), "CONTAINED_PROTOCOL_DRIFT", now);
            contain(task, "PROTOCOL_DRIFT", now);
            return receipt(update.tenantId(), update.remoteWorkId(),
                    "CONTAINED", "PINNED_CONTRACT_MISMATCH");
        }
        if (task.peerTaskId() != null
                && !Objects.equals(task.peerTaskId(), update.peerTaskId())) {
            inbox(update, payloadSha, context.authenticatedPeerRef(), "DENIED_TASK_BINDING", now);
            return receipt(update.tenantId(), update.remoteWorkId(),
                    "DENIED", "PEER_TASK_BINDING_MISMATCH");
        }
        if (update.reportedExternalEffectRef() != null) {
            inbox(update, payloadSha, context.authenticatedPeerRef(), "CONTAINED_EFFECT_REPORT", now);
            contain(task, "REMOTE_EFFECT_OWNERSHIP_VIOLATION", now);
            return receipt(update.tenantId(), update.remoteWorkId(),
                    "CONTAINED", "REMOTE_EFFECT_REQUIRES_MANUAL_OWNERSHIP");
        }
        if (update.cumulativeReportedTokens() < 0
                || update.cumulativeReportedCostMicros() < 0
                || update.cumulativeReportedTokens() < task.reportedTokens()
                || update.cumulativeReportedCostMicros() < task.reportedCostMicros()) {
            inbox(update, payloadSha, context.authenticatedPeerRef(),
                    "CONTAINED_NON_MONOTONIC_USAGE", now);
            contain(task, "NON_MONOTONIC_REMOTE_USAGE_REPORT", now);
            return receipt(update.tenantId(), update.remoteWorkId(),
                    "CONTAINED", "NEGATIVE_OR_REGRESSING_CUMULATIVE_USAGE");
        }
        if (update.cumulativeReportedTokens() > task.maxReportedTokens()
                || update.cumulativeReportedCostMicros() > task.maxReportedCostMicros()) {
            inbox(update, payloadSha, context.authenticatedPeerRef(), "CONTAINED_BUDGET_REPORT", now);
            contain(task, "REMOTE_BUDGET_REPORT_EXCEEDED", now);
            publish(update.tenantId(), update.remoteWorkId(), task.version() + 1,
                    "RemotePeerCancellationRequested", "reason=reported-budget", now);
            return receipt(update.tenantId(), update.remoteWorkId(),
                    "CONTAINED", "LOCAL_CONTAINMENT_DOES_NOT_PROVE_REMOTE_STOP");
        }

        String peerTaskId = update.kind() == RemotePeerUpdate.Kind.REJECTED
                || update.kind() == RemotePeerUpdate.Kind.FAILED
                ? update.peerTaskId()
                : Objects.requireNonNull(update.peerTaskId(), "peerTaskId");
        switch (update.kind()) {
            case ACCEPTED -> transition(task, peerTaskId, "REMOTE_RUNNING", update, now);
            case PARTIAL -> {
                admitArtifact(task, update, "PARTIAL", now);
                transition(task, peerTaskId, "REMOTE_RUNNING", update, now);
            }
            case COMPLETED -> {
                admitArtifact(task, update, "FINAL", now);
                transition(task, peerTaskId, "COMPLETED", update, now);
            }
            case REJECTED -> transition(task, peerTaskId, "REJECTED", update, now);
            case FAILED -> transition(task, peerTaskId, "FAILED", update, now);
            case CANCELLED -> transition(task, peerTaskId, "CANCELLED_CONFIRMED", update, now);
            case INPUT_REQUIRED -> transition(task, peerTaskId, "WAITING_FOR_INPUT", update, now);
            case AUTH_REQUIRED -> transition(task, peerTaskId,
                    "WAITING_FOR_PEER_AUTHORIZATION", update, now);
        }
        inbox(update, payloadSha, context.authenticatedPeerRef(), "ACCEPTED", now);
        return receipt(update.tenantId(), update.remoteWorkId(),
                "ACCEPTED", update.kind().name());
    }
    // end::accept-peer-update-transaction[]

    @Transactional
    public RemoteTaskReceipt requestCancellation(RemoteCancellationRequest request) {
        TaskRow task = load(request.tenantId(), request.remoteWorkId());
        Instant now = clock.instant();
        if (isTerminal(task.state()))
            return receipt(request.tenantId(), request.remoteWorkId(),
                    "NO_OP", "ALREADY_TERMINAL");
        afterRead.afterRead("CANCELLATION", task.tenantId(), task.remoteWorkId(), task.version());
        int changed = jdbc.update("""
                update remote_peer_task set task_state='CANCELLATION_REQUESTED',
                cancellation_reason=?, version=version+1, updated_at=?
                where tenant_id=? and remote_work_id=? and version=?
                """, request.reasonCode(), now, request.tenantId(), request.remoteWorkId(),
                task.version());
        if (changed != 1) throw new IllegalStateException("CONCURRENT_REMOTE_TASK_UPDATE");
        publish(request.tenantId(), request.remoteWorkId(), task.version() + 1,
                "RemotePeerCancellationRequested", "reason=" + request.reasonCode(), now);
        return receipt(request.tenantId(), request.remoteWorkId(),
                "REQUESTED", "CANCELLATION_IS_INTENT_NOT_CONFIRMED_STOP");
    }

    // tag::remote-peer-deadline-transition[]
    @Transactional
    public int expireDue() {
        Instant now = clock.instant();
        List<TaskRow> due = jdbc.query("""
                select tenant_id, remote_work_id, peer_ref, peer_task_id,
                       protocol_family, protocol_version, interaction_profile,
                       deadline_at, max_reported_tokens, max_reported_cost_micros,
                       max_artifact_bytes, reported_tokens, reported_cost_micros,
                       required_deliverables, result_schema_ref, task_state, version
                from remote_peer_task
                where deadline_at <= ? and task_state in
                  ('DISPATCH_PENDING','REMOTE_RUNNING','CANCELLATION_REQUESTED',
                   'WAITING_FOR_INPUT','WAITING_FOR_PEER_AUTHORIZATION')
                """, (rs, n) -> row(rs), now);
        int transitioned = 0;
        for (TaskRow task : due) {
            afterRead.afterRead("EXPIRY", task.tenantId(), task.remoteWorkId(), task.version());
            int changed = jdbc.update("""
                    update remote_peer_task set task_state='TIMED_OUT',
                    cancellation_reason='LOCAL_DEADLINE_EXPIRED', version=version+1, updated_at=?
                    where tenant_id=? and remote_work_id=? and version=?
                    """, now, task.tenantId(), task.remoteWorkId(), task.version());
            if (changed == 1) {
                publish(task.tenantId(), task.remoteWorkId(), task.version() + 1,
                        "RemotePeerWorkTimedOut", "cancellation=attempted", now);
                transitioned++;
            }
        }
        return transitioned;
    }
    // end::remote-peer-deadline-transition[]

    public RemoteTaskReceipt current(String tenantId, String remoteWorkId) {
        return receipt(tenantId, remoteWorkId, "CURRENT", "LOCAL_RECORD");
    }

    private void transition(TaskRow task, String peerTaskId, String state,
            RemotePeerUpdate update, Instant now) {
        int changed = jdbc.update("""
                update remote_peer_task set peer_task_id=?, task_state=?,
                reported_tokens=?, reported_cost_micros=?, version=version+1, updated_at=?
                where tenant_id=? and remote_work_id=? and version=?
                """, peerTaskId, state, update.cumulativeReportedTokens(),
                update.cumulativeReportedCostMicros(), now, task.tenantId(),
                task.remoteWorkId(), task.version());
        if (changed != 1) throw new IllegalStateException("CONCURRENT_REMOTE_TASK_UPDATE");
    }

    private void admitArtifact(TaskRow task, RemotePeerUpdate update, String role, Instant now) {
        if (update.artifactId() == null || update.artifactRef() == null
                || update.artifactSha256() == null
                || !update.artifactSha256().matches("[0-9a-f]{64}")
                || update.artifactSizeBytes() == null || update.artifactSizeBytes() < 0
                || update.artifactSizeBytes() > task.maxArtifactBytes()
                || update.missingDeliverables() == null)
            throw new IllegalArgumentException("INVALID_OR_OVERSIZE_REMOTE_ARTIFACT");
        if (!Objects.equals(task.resultSchemaRef(), update.resultSchemaRef()))
            throw new IllegalArgumentException("REMOTE_RESULT_SCHEMA_MISMATCH");
        if (update.provenanceRef() == null || update.provenanceRef().isBlank()
                || update.evidenceObservedAt() == null || update.evidenceObservedAt().isAfter(now)
                || update.evidenceValidUntil() == null
                || !now.isBefore(update.evidenceValidUntil()))
            throw new IllegalArgumentException("INVALID_OR_STALE_REMOTE_PROVENANCE");
        Set<String> missing = update.missingDeliverables() == null
                ? Set.of() : update.missingDeliverables();
        Set<String> required = Set.of(task.requiredDeliverables().split(","));
        if (!required.containsAll(missing))
            throw new IllegalArgumentException("UNKNOWN_MISSING_DELIVERABLE");
        if ("FINAL".equals(role) && update.missingDeliverables() != null
                && !update.missingDeliverables().isEmpty())
            throw new IllegalArgumentException("FINAL_RESULT_HAS_MISSING_DELIVERABLES");
        jdbc.update("""
                insert into remote_peer_artifact
                (tenant_id, remote_work_id, artifact_id, artifact_ref, artifact_sha256,
                 artifact_size_bytes, result_schema_ref, provenance_ref,
                 evidence_observed_at, evidence_valid_until,
                 artifact_role, missing_deliverables, received_at)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, update.tenantId(), update.remoteWorkId(), update.artifactId(),
                update.artifactRef(), update.artifactSha256(), update.artifactSizeBytes(),
                update.resultSchemaRef(), update.provenanceRef(), update.evidenceObservedAt(),
                update.evidenceValidUntil(), role, canonicalSet(update.missingDeliverables()), now);
    }

    private String contextDenial(ProtectedRemotePeerContext context, TaskRow task,
            RemotePeerUpdate update, Instant now) {
        if (context == null) return "MISSING_PROTECTED_PEER_CONTEXT";
        if (!Objects.equals(context.audience(), AUDIENCE)) return "WRONG_AUDIENCE";
        if (!Objects.equals(context.serviceRef(), SERVICE)) return "WRONG_SERVICE_CONTEXT";
        if (context.expiresAt() == null || !now.isBefore(context.expiresAt()))
            return "STALE_PEER_AUTHENTICATION";
        if (!Objects.equals(context.authenticatedPeerRef(), task.peerRef()))
            return "AUTHENTICATED_PEER_MISMATCH";
        if (!context.permittedTenantIds().contains(task.tenantId()))
            return "TENANT_BOUNDARY_MISMATCH";
        if (!Objects.equals(update.tenantId(), task.tenantId()))
            return "UPDATE_TENANT_MISMATCH";
        return null;
    }

    private void inbox(RemotePeerUpdate update, String sha, String authenticatedPeer,
            String disposition, Instant now) {
        try {
            jdbc.update("""
                    insert into remote_peer_message_inbox
                    (tenant_id, message_id, remote_work_id, payload_sha256,
                     authenticated_peer_ref, disposition, received_at)
                    values (?,?,?,?,?,?,?)
                    """, update.tenantId(), update.messageId(), update.remoteWorkId(), sha,
                    authenticatedPeer, disposition, now);
        } catch (DuplicateKeyException duplicate) {
            throw new IllegalStateException("CONCURRENT_REMOTE_MESSAGE_ID", duplicate);
        }
    }

    private void contain(TaskRow task, String reason, Instant now) {
        afterRead.afterRead("CONTAINMENT", task.tenantId(), task.remoteWorkId(), task.version());
        int changed = jdbc.update("""
                update remote_peer_task set task_state='CONTAINED', containment_reason=?,
                version=version+1, updated_at=? where tenant_id=? and remote_work_id=?
                and version=?
                and task_state not in ('COMPLETED','REJECTED','FAILED','CANCELLED_CONFIRMED',
                                       'TIMED_OUT','CONTAINED')
                """, reason, now, task.tenantId(), task.remoteWorkId(), task.version());
        if (changed != 1) throw new IllegalStateException("CONCURRENT_REMOTE_TASK_UPDATE");
    }

    private void publish(String tenantId, String remoteWorkId, long version,
            String eventType, String payload, Instant now) {
        jdbc.update("""
                insert into remote_peer_outbox
                (event_id, tenant_id, remote_work_id, task_version, event_type,
                 event_payload, created_at) values (?,?,?,?,?,?,?)
                """, "peer-event-" + UUID.randomUUID(), tenantId, remoteWorkId, version,
                eventType, payload, now);
    }

    private RemoteTaskReceipt receipt(
            String tenantId, String remoteWorkId, String disposition, String reason) {
        TaskRow row = load(tenantId, remoteWorkId);
        return new RemoteTaskReceipt(row.remoteWorkId(), row.peerTaskId(), row.state(),
                disposition, reason, row.version());
    }

    private TaskRow load(String tenantId, String remoteWorkId) {
        return jdbc.queryForObject("""
                select tenant_id, remote_work_id, peer_ref, peer_task_id,
                       protocol_family, protocol_version, interaction_profile,
                       deadline_at, max_reported_tokens, max_reported_cost_micros,
                       max_artifact_bytes, reported_tokens, reported_cost_micros,
                       required_deliverables, result_schema_ref, task_state, version
                from remote_peer_task where tenant_id=? and remote_work_id=?
                """, (rs, n) -> row(rs), tenantId, remoteWorkId);
    }

    private static TaskRow row(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new TaskRow(rs.getString("tenant_id"), rs.getString("remote_work_id"),
                rs.getString("peer_ref"), rs.getString("peer_task_id"),
                rs.getString("protocol_family"), rs.getString("protocol_version"),
                rs.getString("interaction_profile"),
                rs.getObject("deadline_at", java.time.OffsetDateTime.class).toInstant(),
                rs.getLong("max_reported_tokens"), rs.getLong("max_reported_cost_micros"),
                rs.getInt("max_artifact_bytes"), rs.getLong("reported_tokens"),
                rs.getLong("reported_cost_micros"), rs.getString("required_deliverables"),
                rs.getString("result_schema_ref"), rs.getString("task_state"),
                rs.getLong("version"));
    }

    private static String updateSha256(RemotePeerUpdate u) {
        return sha256(String.join("|", String.valueOf(u.messageId()),
                String.valueOf(u.tenantId()), String.valueOf(u.remoteWorkId()),
                String.valueOf(u.peerTaskId()), String.valueOf(u.kind()),
                String.valueOf(u.protocolFamily()), String.valueOf(u.protocolVersion()),
                String.valueOf(u.interactionProfile()),
                String.valueOf(u.cumulativeReportedTokens()),
                String.valueOf(u.cumulativeReportedCostMicros()),
                String.valueOf(u.artifactId()), String.valueOf(u.artifactRef()),
                String.valueOf(u.artifactSha256()), String.valueOf(u.artifactSizeBytes()),
                String.valueOf(u.resultSchemaRef()), String.valueOf(u.provenanceRef()),
                String.valueOf(u.evidenceObservedAt()), String.valueOf(u.evidenceValidUntil()),
                canonicalSet(u.missingDeliverables()),
                String.valueOf(u.reportedExternalEffectRef())));
    }

    private static String collisionEvidence(String originalSha, String receivedSha) {
        return "originalSha=" + originalSha + ";receivedSha=" + receivedSha;
    }

    private static boolean isTerminal(String state) {
        return switch (state) {
            case "COMPLETED", "REJECTED", "FAILED", "CANCELLED_CONFIRMED",
                    "TIMED_OUT", "CONTAINED" -> true;
            default -> false;
        };
    }

    private static void requireTask(RemoteTaskDefinition task) {
        if (task == null || task.tenantId() == null || task.remoteWorkId() == null
                || task.caseId() == null || task.correlationId() == null
                || task.localOwnerRef() == null || task.deadlineAt() == null
                || task.objective() == null || task.objective().isBlank()
                || task.requiredDeliverables() == null || task.requiredDeliverables().isEmpty()
                || task.maxReportedTokens() < 0 || task.maxReportedCostMicros() < 0
                || task.maxArtifactBytes() <= 0 || task.requiredCapability() == null
                || task.resultSchemaRef() == null || task.resultSchemaRef().isBlank()
                || task.inputArtifactRef() == null || task.inputArtifactSha256() == null
                || !task.inputArtifactSha256().matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("INVALID_REMOTE_TASK_DEFINITION");
    }

    private record TaskRow(
            String tenantId, String remoteWorkId, String peerRef, String peerTaskId,
            String protocolFamily, String protocolVersion, String interactionProfile,
            Instant deadlineAt, long maxReportedTokens, long maxReportedCostMicros,
            int maxArtifactBytes, long reportedTokens, long reportedCostMicros,
            String requiredDeliverables, String resultSchemaRef, String state, long version) {
    }

    private record InboxRow(
            String payloadSha256, String remoteWorkId, String authenticatedPeerRef) {
    }

    private static String canonicalSet(Set<String> values) {
        return values == null ? "" : String.join(",", new TreeSet<>(values));
    }
}
