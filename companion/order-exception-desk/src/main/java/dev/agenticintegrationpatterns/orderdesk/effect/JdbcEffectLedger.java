package dev.agenticintegrationpatterns.orderdesk.effect;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt.State.*;

/**
 * A local teaching ledger assembled from established transaction, idempotency,
 * outbox, optimistic-version, and reconciliation practices.
 */
@Component
public class JdbcEffectLedger {
    private static final String EFFECT_TYPE = "RESERVE_INVENTORY";
    private static final String TARGET_SYSTEM = "warehouse-api";
    private static final String SPLIT_SHIPMENT_TYPE = "CREATE_SPLIT_SHIPMENT";
    private static final String RELEASE_TYPE = "RELEASE_INVENTORY_RESERVATION";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final AfterEffectStateHook afterState;
    private final EffectLedgerConfiguration.TargetIdempotencyPolicy targetPolicy;

    public JdbcEffectLedger(
            JdbcTemplate jdbc,
            ObjectMapper mapper,
            Clock clock,
            AfterEffectStateHook afterState,
            EffectLedgerConfiguration.TargetIdempotencyPolicy targetPolicy) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.clock = clock;
        this.afterState = afterState;
        this.targetPolicy = targetPolicy;
    }

    // tag::register-effect-transaction[]
    @Transactional
    public EffectReceipt register(ReserveInventoryEffect effect) {
        requireRunIdentity(effect);
        String fingerprint = intentSha256(effect);
        var existing = find(effect.tenantId(), effect.effectId());
        if (existing.isPresent()) {
            if (existing.get().intentSha256().equals(fingerprint)) {
                return receipt(DUPLICATE_SAME, existing.get());
            }
            jdbc.update("""
                    insert into effect_identity_collision
                    (tenant_id, effect_id, existing_sha256, received_sha256, observed_at)
                    values (?, ?, ?, ?, ?)
                    """, effect.tenantId(), effect.effectId(),
                    existing.get().intentSha256(), fingerprint, clock.instant());
            return receipt(IDENTITY_COLLISION, existing.get());
        }

        return insertNew(new NewEffect(
                effect.tenantId(), effect.runId(), effect.caseId(), effect.effectId(),
                effect.decisionRef(), effect.policySnapshotRef(), EFFECT_TYPE, TARGET_SYSTEM,
                effect.targetResourceKey(), effect.warehouseId(), effect.sku(), effect.quantity(),
                null, null, 0, fingerprint, targetPolicy.contractRef(), null, null, null,
                null, null, null));
    }
    // end::register-effect-transaction[]

    /** Canonical digest shared by approval binding and the durable effect ledger. */
    public String intentSha256(ReserveInventoryEffect effect) {
        return fingerprint(new CanonicalIntent(
                effect.tenantId(), effect.runId(), effect.caseId(), effect.effectId(),
                effect.decisionRef(), effect.policySnapshotRef(), effect.warehouseId(),
                effect.sku(), effect.quantity()));
    }

    /** Register the later forward effect without pretending that it shares the reservation contract. */
    @Transactional
    public EffectReceipt register(CreateSplitShipmentEffect effect) {
        requireRunIdentity(effect.tenantId(), effect.runId(), effect.caseId());
        if (!clock.instant().isBefore(effect.authorityValidUntil())) {
            throw new IllegalStateException("split-shipment authority is expired");
        }
        var predecessor = find(effect.tenantId(), effect.causedByEffectId()).orElseThrow(
                () -> new IllegalArgumentException("causing reservation effect does not exist"));
        if (predecessor.state() == FAILED_CONFIRMED || predecessor.state() == UNKNOWN) {
            throw new IllegalStateException("split shipment cannot follow an unresolved reservation");
        }
        String fingerprint = fingerprint(List.of(
                effect.tenantId(), effect.runId(), effect.caseId(), effect.effectId(),
                effect.decisionRef(), effect.policySnapshotRef(), effect.causedByEffectId(),
                effect.authorityRef(), effect.evidenceSha256(), effect.configurationRef(),
                effect.orderId(), effect.expectedOrderVersion(), effect.reservationReference(), effect.warehouseId(),
                effect.sku(), effect.quantity()));
        var duplicate = existing(effect.tenantId(), effect.effectId(), fingerprint);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }
        return insertNew(new NewEffect(
                effect.tenantId(), effect.runId(), effect.caseId(), effect.effectId(),
                effect.decisionRef(), effect.policySnapshotRef(), SPLIT_SHIPMENT_TYPE,
                "fulfillment-api", effect.targetResourceKey(), effect.warehouseId(),
                effect.sku(), effect.quantity(), effect.orderId(), effect.reservationReference(),
                effect.expectedOrderVersion(), fingerprint, "order-split-shipment-v1",
                effect.causedByEffectId(), null, effect.authorityRef(),
                effect.authorityValidUntil(), effect.evidenceSha256(), effect.configurationRef()));
    }

    /** Register compensation as a new governed effect, never by mutating the original effect. */
    // tag::register-governed-compensation[]
    @Transactional
    public EffectReceipt register(ReleaseInventoryReservationEffect effect) {
        requireRunIdentity(effect.tenantId(), effect.runId(), effect.caseId());
        var original = find(effect.tenantId(), effect.compensatesEffectId()).orElseThrow(
                () -> new IllegalArgumentException("compensated effect does not exist"));
        if (original.state() != SUCCEEDED) {
            throw new IllegalStateException("only a confirmed successful effect can be compensated");
        }
        if (!original.runId().equals(effect.runId())
                || !original.caseId().equals(effect.caseId())
                || !original.warehouseId().equals(effect.warehouseId())
                || !original.sku().equals(effect.sku())
                || original.quantity() != effect.quantity()
                || !effect.reservationReference().equals(original.targetReference())) {
            throw new IllegalArgumentException("compensation target differs from original effect");
        }
        if (!clock.instant().isBefore(effect.authority().validUntil())) {
            throw new IllegalStateException("recovery authority is expired");
        }
        String fingerprint = fingerprint(List.of(
                effect.tenantId(), effect.runId(), effect.caseId(), effect.planId(),
                effect.effectId(), effect.causedByEffectId(), effect.compensatesEffectId(),
                effect.decisionRef(), effect.policySnapshotRef(), effect.targetContractRef(),
                effect.reservationReference(), effect.warehouseId(), effect.sku(), effect.quantity(),
                effect.authority()));
        var duplicate = existing(effect.tenantId(), effect.effectId(), fingerprint);
        if (duplicate.isPresent()) {
            return duplicate.get();
        }
        return insertNew(new NewEffect(
                effect.tenantId(), effect.runId(), effect.caseId(), effect.effectId(),
                effect.decisionRef(), effect.policySnapshotRef(), RELEASE_TYPE, TARGET_SYSTEM,
                effect.targetResourceKey(), effect.warehouseId(), effect.sku(), effect.quantity(),
                null, effect.reservationReference(), 0, fingerprint, effect.targetContractRef(), effect.causedByEffectId(),
                effect.compensatesEffectId(), effect.authority().authorityId(),
                effect.authority().validUntil(), effect.authority().evidenceSha256(),
                effect.authority().configurationRef()));
    }
    // end::register-governed-compensation[]

    // tag::claim-effect-attempt[]
    @Transactional
    public Optional<EffectLease> claimRecorded(
            String tenantId, String effectId, String owner, Duration leaseDuration) {
        ReserveInventoryEffect.requireText(tenantId, "tenantId", 120);
        ReserveInventoryEffect.requireText(effectId, "effectId", 160);
        ReserveInventoryEffect.requireText(owner, "owner", 200);
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        var current = find(tenantId, effectId);
        if (current.isEmpty() || current.get().state() != RECORDED) {
            return Optional.empty();
        }

        var row = current.get();
        Instant startedAt = clock.instant();
        Instant leaseUntil = startedAt.plus(leaseDuration);
        Instant keyExpiresAt = startedAt.plus(targetPolicy.retention());
        int attemptNumber = row.attemptCount() + 1;
        long nextVersion = row.version() + 1;
        long nextFence = row.fenceToken() + 1;
        String attemptId = effectId + ":attempt:" + attemptNumber;

        int claimed = jdbc.update("""
                update effect_ledger
                   set effect_state='DISPATCHING', version=?, attempt_count=?,
                       lease_owner=?, lease_until=?, fence_token=?,
                       idempotency_expires_at=?, updated_at=?
                 where tenant_id=? and effect_id=? and effect_state='RECORDED' and version=?
                """, nextVersion, attemptNumber, owner, leaseUntil, nextFence,
                keyExpiresAt, startedAt, tenantId, effectId, row.version());
        if (claimed != 1) {
            return Optional.empty();
        }
        jdbc.update("""
                insert into effect_attempt
                (tenant_id, effect_id, attempt_number, attempt_id, lease_owner,
                 fence_token, attempt_state, started_at, lease_until)
                values (?, ?, ?, ?, ?, ?, 'DISPATCHING', ?, ?)
                """, tenantId, effectId, attemptNumber, attemptId, owner,
                nextFence, startedAt, leaseUntil);
        afterState.afterStateMutation(tenantId, effectId, DISPATCHING, nextVersion);
        insertOutbox(tenantId, effectId, row.runId(), row.caseId(),
                row.idempotencyKey(), nextVersion, "EffectAttemptStarted",
                DISPATCHING, attemptId, startedAt);
        return Optional.of(new EffectLease(
                tenantId, row.runId(), row.caseId(), effectId, attemptId,
                attemptNumber, owner, nextVersion, nextFence, leaseUntil,
                row.idempotencyKey(), keyExpiresAt, row.effectType(), row.warehouseId(),
                row.sku(), row.quantity(), row.orderId(), row.expectedOrderVersion(),
                row.reservationReference()));
    }
    // end::claim-effect-attempt[]

    @Transactional
    public EffectReceipt recordInvocation(
            EffectLease lease,
            InventoryReservationClient.InvocationResult result) {
        var next = switch (result.outcome()) {
            case ACCEPTED -> ACCEPTED;
            case SUCCEEDED -> SUCCEEDED;
            case FAILED_CONFIRMED -> FAILED_CONFIRMED;
            case UNKNOWN -> throw new IllegalArgumentException(
                    "unknown invocation outcome must use recordUnknown");
        };
        return finishAttempt(lease, next, result.targetReference(),
                result.evidenceRef(), "TARGET_" + result.outcome().name());
    }

    @Transactional
    public EffectReceipt recordUnknown(
            EffectLease lease, String reasonCode, String evidenceRef) {
        ReserveInventoryEffect.requireText(reasonCode, "reasonCode", 120);
        ReserveInventoryEffect.requireText(evidenceRef, "evidenceRef", 600);
        return finishAttempt(lease, UNKNOWN, null, evidenceRef, reasonCode);
    }

    private EffectReceipt finishAttempt(
            EffectLease lease,
            EffectReceipt.State next,
            String targetReference,
            String evidenceRef,
            String reasonCode) {
        var current = find(lease.tenantId(), lease.effectId()).orElseThrow(
                () -> new IllegalArgumentException("unknown effect"));
        Instant finishedAt = clock.instant();
        if (current.state() != DISPATCHING
                || current.version() != lease.version()
                || current.fenceToken() != lease.fenceToken()
                || !lease.owner().equals(current.leaseOwner())
                || current.leaseUntil() == null
                || !current.leaseUntil().isAfter(finishedAt)) {
            throw new StaleEffectLeaseException(
                    "effect attempt cannot commit with an expired or stale lease");
        }
        long nextVersion = current.version() + 1;
        int updated = jdbc.update("""
                update effect_ledger
                   set effect_state=?, version=?, lease_owner=null, lease_until=null,
                       target_reference=?, resolution_evidence_ref=?, updated_at=?
                 where tenant_id=? and effect_id=? and effect_state='DISPATCHING'
                   and version=? and fence_token=? and lease_owner=? and lease_until>?
                """, next.name(), nextVersion, targetReference, evidenceRef, finishedAt,
                lease.tenantId(), lease.effectId(), lease.version(), lease.fenceToken(),
                lease.owner(), finishedAt);
        if (updated != 1) {
            throw new StaleEffectLeaseException("effect changed before outcome commit");
        }
        int attemptUpdated = jdbc.update("""
                update effect_attempt
                   set attempt_state=?, finished_at=?, reason_code=?, evidence_ref=?,
                       target_reference=?
                 where tenant_id=? and effect_id=? and attempt_number=?
                   and attempt_id=? and attempt_state='DISPATCHING'
                """, next.name(), finishedAt, reasonCode, evidenceRef, targetReference,
                lease.tenantId(), lease.effectId(), lease.attemptNumber(), lease.attemptId());
        if (attemptUpdated != 1) {
            throw new StaleEffectLeaseException(
                    "effect attempt changed before outcome commit");
        }
        afterState.afterStateMutation(
                lease.tenantId(), lease.effectId(), next, nextVersion);
        insertOutbox(lease.tenantId(), lease.effectId(), lease.runId(), lease.caseId(),
                lease.targetIdempotencyKey(), nextVersion,
                "EffectOutcome" + eventSuffix(next), next,
                lease.attemptId(), finishedAt);
        return receipt(OUTCOME_RECORDED,
                find(lease.tenantId(), lease.effectId()).orElseThrow());
    }

    // tag::expired-attempt-becomes-unknown[]
    @Transactional
    public Optional<EffectReceipt> recoverExpiredDispatch(
            String tenantId, String effectId, String evidenceRef) {
        ReserveInventoryEffect.requireText(evidenceRef, "evidenceRef", 600);
        var current = find(tenantId, effectId);
        Instant recoveredAt = clock.instant();
        if (current.isEmpty() || current.get().state() != DISPATCHING
                || current.get().leaseUntil() == null
                || current.get().leaseUntil().isAfter(recoveredAt)) {
            return Optional.empty();
        }
        var row = current.get();
        long nextVersion = row.version() + 1;
        int updated = jdbc.update("""
                update effect_ledger
                   set effect_state='UNKNOWN', version=?, lease_owner=null,
                       lease_until=null, resolution_evidence_ref=?, updated_at=?
                 where tenant_id=? and effect_id=? and effect_state='DISPATCHING'
                   and version=? and fence_token=? and lease_until<=?
                """, nextVersion, evidenceRef, recoveredAt, tenantId, effectId,
                row.version(), row.fenceToken(), recoveredAt);
        if (updated != 1) {
            return Optional.empty();
        }
        jdbc.update("""
                update effect_attempt
                   set attempt_state='UNKNOWN', finished_at=?,
                       reason_code='WORKER_LOST_AFTER_DISPATCH_CLAIM', evidence_ref=?
                 where tenant_id=? and effect_id=? and attempt_number=?
                   and attempt_state='DISPATCHING'
                """, recoveredAt, evidenceRef, tenantId, effectId, row.attemptCount());
        afterState.afterStateMutation(tenantId, effectId, UNKNOWN, nextVersion);
        insertOutbox(tenantId, effectId, row.runId(), row.caseId(), row.idempotencyKey(),
                nextVersion, "EffectOutcomeUnknown", UNKNOWN,
                effectId + ":attempt:" + row.attemptCount(), recoveredAt);
        return Optional.of(receipt(OUTCOME_RECORDED,
                find(tenantId, effectId).orElseThrow()));
    }
    // end::expired-attempt-becomes-unknown[]

    // tag::reconcile-effect[]
    @Transactional
    public EffectReceipt reconcile(
            String tenantId,
            String effectId,
            String observationId,
            InventoryReservationClient.TargetObservation observation) {
        ReserveInventoryEffect.requireText(observationId, "observationId", 200);
        ReserveInventoryEffect.requireText(
                observation.evidenceRef(), "evidenceRef", 600);
        var current = find(tenantId, effectId).orElseThrow(
                () -> new IllegalArgumentException("unknown effect"));
        String observationSha = fingerprint(List.of(
                observation.outcome(), nullable(observation.targetReference()),
                observation.evidenceRef()));
        var existingObservation = jdbc.query("""
                select observation_sha256
                  from effect_reconciliation_observation
                 where tenant_id=? and effect_id=? and observation_id=?
                """, (rs, row) -> rs.getString(1), tenantId, effectId, observationId);
        if (!existingObservation.isEmpty()) {
            if (!existingObservation.get(0).equals(observationSha)) {
                throw new IllegalArgumentException(
                        "reconciliation observation identity collision");
            }
            return receipt(RECONCILIATION_RECORDED, current);
        }
        if (current.state() != UNKNOWN && current.state() != ACCEPTED) {
            throw new IllegalStateException(
                    "only UNKNOWN or ACCEPTED effects can be reconciled");
        }

        Instant observedAt = clock.instant();
        jdbc.update("""
                insert into effect_reconciliation_observation
                (tenant_id, effect_id, observation_id, observation_sha256,
                 observed_outcome, target_reference, evidence_ref, observed_at)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """, tenantId, effectId, observationId, observationSha,
                observation.outcome().name(), observation.targetReference(),
                observation.evidenceRef(), observedAt);
        var next = switch (observation.outcome()) {
            case SUCCEEDED -> SUCCEEDED;
            case FAILED_CONFIRMED -> FAILED_CONFIRMED;
            case ACCEPTED -> ACCEPTED;
            case UNKNOWN -> current.state();
        };
        long nextVersion = current.version() + 1;
        int ledgerUpdated = jdbc.update("""
                update effect_ledger
                   set effect_state=?, version=?, target_reference=?,
                       resolution_evidence_ref=?, updated_at=?
                 where tenant_id=? and effect_id=? and version=?
                   and effect_state in ('UNKNOWN', 'ACCEPTED')
                """, next.name(), nextVersion,
                observation.targetReference() == null
                        ? current.targetReference() : observation.targetReference(),
                observation.evidenceRef(), observedAt,
                tenantId, effectId, current.version());
        if (ledgerUpdated != 1) {
            throw new ConcurrentEffectUpdateException(
                    "effect changed before reconciliation commit");
        }
        afterState.afterStateMutation(tenantId, effectId, next, nextVersion);
        insertOutbox(tenantId, effectId, current.runId(), current.caseId(),
                current.idempotencyKey(), nextVersion, "EffectReconciliationObserved",
                next, observationId, observedAt);
        return receipt(RECONCILIATION_RECORDED,
                find(tenantId, effectId).orElseThrow());
    }
    // end::reconcile-effect[]

    public EffectReceipt current(String tenantId, String effectId) {
        return find(tenantId, effectId)
                .map(row -> receipt(NO_EXECUTION, row))
                .orElseThrow(() -> new IllegalArgumentException("unknown effect"));
    }

    public EffectSnapshot snapshot(String tenantId, String effectId) {
        var row = find(tenantId, effectId).orElseThrow(
                () -> new IllegalArgumentException("unknown effect"));
        return new EffectSnapshot(row.tenantId(), row.effectId(), row.runId(), row.caseId(),
                row.effectType(), row.targetContractRef(), row.state(), row.version(),
                row.causedByEffectId(), row.compensatesEffectId(), row.authorityRef(),
                row.authorityValidUntil(), row.authorityEvidenceSha256(),
                row.authorityConfigurationRef(), row.targetReference(),
                row.resolutionEvidenceRef());
    }

    public TargetLookup targetLookup(String tenantId, String effectId) {
        var row = find(tenantId, effectId).orElseThrow(
                () -> new IllegalArgumentException("unknown effect"));
        return new TargetLookup(row.tenantId(), row.effectId(), row.idempotencyKey());
    }

    private void requireRunIdentity(ReserveInventoryEffect effect) {
        requireRunIdentity(effect.tenantId(), effect.runId(), effect.caseId());
    }

    private void requireRunIdentity(String tenantId, String runId, String caseId) {
        var cases = jdbc.query("""
                select case_id from investigation_run where tenant_id=? and run_id=?
                """, (rs, row) -> rs.getString(1), tenantId, runId);
        if (cases.isEmpty() || !cases.get(0).equals(caseId)) {
            throw new IllegalArgumentException(
                    "effect must reference an existing tenant-scoped run and case");
        }
    }

    private Optional<EffectRow> find(String tenantId, String effectId) {
        ReserveInventoryEffect.requireText(tenantId, "tenantId", 120);
        ReserveInventoryEffect.requireText(effectId, "effectId", 160);
        return jdbc.query("""
                select tenant_id, effect_id, run_id, case_id, effect_type,
                       warehouse_id, sku, quantity, order_id, expected_order_version,
                       reservation_reference,
                       intent_sha256, target_contract_ref, target_idempotency_key,
                       idempotency_expires_at, effect_state, version, attempt_count,
                       lease_owner, lease_until, fence_token, target_reference,
                       resolution_evidence_ref,
                       caused_by_effect_id, compensates_effect_id, authority_ref,
                       authority_valid_until, authority_evidence_sha256,
                       authority_configuration_ref
                  from effect_ledger where tenant_id=? and effect_id=?
                """, (rs, row) -> new EffectRow(
                        rs.getString("tenant_id"), rs.getString("effect_id"),
                        rs.getString("run_id"), rs.getString("case_id"),
                        rs.getString("effect_type"), rs.getString("warehouse_id"),
                        rs.getString("sku"), rs.getInt("quantity"),
                        rs.getString("order_id"), rs.getLong("expected_order_version"),
                        rs.getString("reservation_reference"),
                        rs.getString("intent_sha256"), rs.getString("target_contract_ref"),
                        rs.getString("target_idempotency_key"),
                        rs.getObject("idempotency_expires_at", Instant.class),
                        EffectReceipt.State.valueOf(rs.getString("effect_state")),
                        rs.getLong("version"), rs.getInt("attempt_count"),
                        rs.getString("lease_owner"),
                        rs.getObject("lease_until", Instant.class),
                        rs.getLong("fence_token"), rs.getString("target_reference"),
                        rs.getString("resolution_evidence_ref"),
                        rs.getString("caused_by_effect_id"),
                        rs.getString("compensates_effect_id"), rs.getString("authority_ref"),
                        rs.getObject("authority_valid_until", Instant.class),
                        rs.getString("authority_evidence_sha256"),
                        rs.getString("authority_configuration_ref")),
                tenantId, effectId).stream().findFirst();
    }

    private Optional<EffectReceipt> existing(
            String tenantId, String effectId, String fingerprint) {
        var existing = find(tenantId, effectId);
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        if (existing.get().intentSha256().equals(fingerprint)) {
            return Optional.of(receipt(DUPLICATE_SAME, existing.get()));
        }
        jdbc.update("""
                insert into effect_identity_collision
                (tenant_id, effect_id, existing_sha256, received_sha256, observed_at)
                values (?, ?, ?, ?, ?)
                """, tenantId, effectId, existing.get().intentSha256(), fingerprint,
                clock.instant());
        return Optional.of(receipt(IDENTITY_COLLISION, existing.get()));
    }

    private EffectReceipt insertNew(NewEffect effect) {
        Instant recordedAt = clock.instant();
        String idempotencyKey = targetIdempotencyKey(effect.tenantId(), effect.effectId());
        jdbc.update("""
                insert into effect_ledger
                (tenant_id, effect_id, run_id, case_id, decision_ref, policy_snapshot_ref,
                 effect_type, target_system, target_resource_key, warehouse_id, sku,
                 quantity, order_id, expected_order_version, reservation_reference, intent_sha256,
                 target_contract_ref, target_idempotency_key, idempotency_expires_at,
                 effect_state, version, attempt_count, lease_owner, lease_until, fence_token,
                 target_reference, resolution_evidence_ref, caused_by_effect_id,
                 compensates_effect_id, authority_ref, authority_valid_until,
                 authority_evidence_sha256, authority_configuration_ref, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null,
                        'RECORDED', 0, 0, null, null, 0, null, null, ?, ?, ?, ?, ?, ?, ?, ?)
                """, effect.tenantId(), effect.effectId(), effect.runId(), effect.caseId(),
                effect.decisionRef(), effect.policySnapshotRef(), effect.effectType(),
                effect.targetSystem(), effect.targetResourceKey(), effect.warehouseId(),
                effect.sku(), effect.quantity(), effect.orderId(), effect.expectedOrderVersion(),
                effect.reservationReference(),
                effect.intentSha256(), effect.targetContractRef(), idempotencyKey,
                effect.causedByEffectId(), effect.compensatesEffectId(), effect.authorityRef(),
                effect.authorityValidUntil(), effect.authorityEvidenceSha256(),
                effect.authorityConfigurationRef(), recordedAt, recordedAt);
        insertOutbox(effect.tenantId(), effect.effectId(), effect.runId(), effect.caseId(),
                idempotencyKey, 0, "EffectRecorded", RECORDED, null, recordedAt);
        afterState.afterStateMutation(effect.tenantId(), effect.effectId(), RECORDED, 0);
        return receipt(CREATED, find(effect.tenantId(), effect.effectId()).orElseThrow());
    }

    private EffectReceipt receipt(EffectReceipt.Disposition disposition, EffectRow row) {
        return new EffectReceipt(
                disposition, row.tenantId(), row.effectId(), row.state(), row.version(),
                row.attemptCount(), row.idempotencyKey(), row.idempotencyExpiresAt(),
                row.targetReference());
    }

    private void insertOutbox(
            String tenantId,
            String effectId,
            String runId,
            String caseId,
            String idempotencyKey,
            long version,
            String eventType,
            EffectReceipt.State state,
            String evidenceIdentity,
            Instant createdAt) {
        var payload = new LinkedHashMap<String, Object>();
        payload.put("tenantId", tenantId);
        payload.put("effectId", effectId);
        payload.put("runId", runId);
        payload.put("caseId", caseId);
        payload.put("effectVersion", version);
        payload.put("state", state.name());
        payload.put("evidenceIdentity", evidenceIdentity);
        String eventId = idempotencyKey + ":v" + version + ":" + eventType;
        jdbc.update("""
                insert into effect_outbox
                (event_id, tenant_id, effect_id, run_id, case_id, effect_version,
                 event_type, event_payload, created_at, published_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, null)
                """, eventId, tenantId, effectId, runId, caseId, version,
                eventType, json(payload), createdAt);
    }

    private String fingerprint(Object value) {
        try {
            return sha256(mapper.writeValueAsBytes(value));
        } catch (Exception failure) {
            throw new IllegalStateException("cannot fingerprint effect data", failure);
        }
    }

    private static String targetIdempotencyKey(String tenantId, String effectId) {
        String scoped = tenantId.length() + ":" + tenantId
                + effectId.length() + ":" + effectId;
        return "fx1_" + sha256(scoped.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private String json(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception failure) {
            throw new IllegalStateException("cannot serialize effect outbox event", failure);
        }
    }

    private static String nullable(String value) {
        return value == null ? "<null>" : value;
    }

    private static String eventSuffix(EffectReceipt.State state) {
        return switch (state) {
            case ACCEPTED -> "Accepted";
            case SUCCEEDED -> "Succeeded";
            case FAILED_CONFIRMED -> "FailedConfirmed";
            case UNKNOWN -> "Unknown";
            default -> throw new IllegalArgumentException("not an invocation outcome: " + state);
        };
    }

    public record TargetLookup(
            String tenantId, String effectId, String targetIdempotencyKey) { }

    public record EffectSnapshot(
            String tenantId, String effectId, String runId, String caseId,
            String effectType, String targetContractRef, EffectReceipt.State state,
            long version, String causedByEffectId, String compensatesEffectId,
            String authorityRef, Instant authorityValidUntil,
            String authorityEvidenceSha256, String authorityConfigurationRef,
            String targetReference, String resolutionEvidenceRef) { }

    private record CanonicalIntent(
            String tenantId,
            String runId,
            String caseId,
            String effectId,
            String decisionRef,
            String policySnapshotRef,
            String warehouseId,
            String sku,
            int quantity) { }

    private record EffectRow(
            String tenantId,
            String effectId,
            String runId,
            String caseId,
            String effectType,
            String warehouseId,
            String sku,
            int quantity,
            String orderId,
            long expectedOrderVersion,
            String reservationReference,
            String intentSha256,
            String targetContractRef,
            String idempotencyKey,
            Instant idempotencyExpiresAt,
            EffectReceipt.State state,
            long version,
            int attemptCount,
            String leaseOwner,
            Instant leaseUntil,
            long fenceToken,
            String targetReference,
            String resolutionEvidenceRef,
            String causedByEffectId,
            String compensatesEffectId,
            String authorityRef,
            Instant authorityValidUntil,
            String authorityEvidenceSha256,
            String authorityConfigurationRef) { }

    private record NewEffect(
            String tenantId, String runId, String caseId, String effectId,
            String decisionRef, String policySnapshotRef, String effectType,
            String targetSystem, String targetResourceKey, String warehouseId,
            String sku, int quantity, String orderId, String reservationReference,
            long expectedOrderVersion,
            String intentSha256, String targetContractRef, String causedByEffectId,
            String compensatesEffectId, String authorityRef, Instant authorityValidUntil,
            String authorityEvidenceSha256, String authorityConfigurationRef) { }

    public static final class StaleEffectLeaseException extends RuntimeException {
        public StaleEffectLeaseException(String message) {
            super(message);
        }
    }

    public static final class ConcurrentEffectUpdateException extends RuntimeException {
        public ConcurrentEffectUpdateException(String message) {
            super(message);
        }
    }
}
