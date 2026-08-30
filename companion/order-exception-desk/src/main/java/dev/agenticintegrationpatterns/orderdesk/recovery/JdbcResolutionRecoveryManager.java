package dev.agenticintegrationpatterns.orderdesk.recovery;

import dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt;
import dev.agenticintegrationpatterns.orderdesk.effect.JdbcEffectLedger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static dev.agenticintegrationpatterns.orderdesk.recovery.ResolutionReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.recovery.ResolutionReceipt.State.*;

/** Application-specific durable recovery coordinator for the Order Exception Desk. */
@Component
public class JdbcResolutionRecoveryManager {
    private final JdbcTemplate jdbc;
    private final JdbcEffectLedger effects;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final AfterResolutionStateHook afterState;

    public JdbcResolutionRecoveryManager(
            JdbcTemplate jdbc, JdbcEffectLedger effects, ObjectMapper mapper,
            Clock clock, AfterResolutionStateHook afterState) {
        this.jdbc = jdbc;
        this.effects = effects;
        this.mapper = mapper;
        this.clock = clock;
        this.afterState = afterState;
    }

    @Transactional
    public ResolutionReceipt open(ResolutionPlanDefinition definition) {
        String definitionSha = fingerprint(definition);
        var existing = find(definition.tenantId(), definition.planId(), false);
        if (!existing.isEmpty()) {
            return receipt(existing.get(0).definitionSha256().equals(definitionSha)
                    ? DUPLICATE_SAME : IDENTITY_COLLISION, existing.get(0));
        }
        validateDistinctOrderedEffects(definition);
        Instant now = clock.instant();
        if (!now.isBefore(definition.deadlineAt())) {
            throw new IllegalArgumentException("resolution plan deadline has passed");
        }
        for (var step : definition.effects()) {
            var effect = effects.snapshot(definition.tenantId(), step.effectId());
            requirePlanEffect(definition, step, effect);
        }
        jdbc.update("""
                insert into resolution_plan
                (tenant_id, plan_id, run_id, case_id, proposal_id, plan_state,
                 recovery_owner, deadline_at, evidence_set_ref, evidence_sha256,
                 configuration_ref, definition_sha256, next_event_number, last_event_id, version,
                 created_at, updated_at)
                values (?,?,?,?,?,'FORWARD_RUNNING',?,?,?,?,?,?,?,?,0,?,?)
                """, definition.tenantId(), definition.planId(), definition.runId(),
                definition.caseId(), definition.proposalId(), definition.recoveryOwner(),
                definition.deadlineAt(), definition.evidenceSetRef(),
                definition.evidenceSha256(), definition.configurationRef(), definitionSha,
                definition.firstEventNumber() + 1,
                eventId(definition.firstEventNumber()), now, now);
        for (var step : definition.effects()) {
            var snapshot = effects.snapshot(definition.tenantId(), step.effectId());
            jdbc.update("""
                    insert into resolution_plan_effect
                    (tenant_id, plan_id, step_number, effect_id, effect_role, effect_type,
                     caused_by_effect_id, compensates_effect_id, target_contract_ref,
                     authority_ref, authority_valid_until, evidence_sha256,
                     configuration_ref, reversibility, observed_state,
                     observed_effect_version, observation_evidence_ref)
                    values (?,?,?,?,'FORWARD',?,?,null,?,?,?,?,?,?,?,?,'plan-open')
                    """, definition.tenantId(), definition.planId(), step.stepNumber(),
                    step.effectId(), step.effectType(), step.causedByEffectId(),
                    step.targetContractRef(), step.authorityRef(), step.authorityValidUntil(),
                    definition.evidenceSha256(), definition.configurationRef(),
                    step.reversibility().name(), snapshot.state().name(), snapshot.version());
        }
        afterState.afterStateMutation(definition.tenantId(), definition.planId(),
                FORWARD_RUNNING, 0);
        outbox(definition.tenantId(), definition.planId(), definition.firstEventNumber(),
                0, "ResolutionPlanStarted", FORWARD_RUNNING, now,
                definition.proposalEventId(), Map.of(
                        "runId", definition.runId(),
                        "caseId", definition.caseId(),
                        "proposalId", definition.proposalId(),
                        "proposalEventId", definition.proposalEventId(),
                        "evidenceSetRef", definition.evidenceSetRef(),
                        "evidenceSha256", definition.evidenceSha256(),
                        "configurationRef", definition.configurationRef(),
                        "forwardEffectIds", definition.effects().stream()
                                .map(ResolutionPlanDefinition.ForwardEffect::effectId).toList()));
        return currentWith(CREATED, definition.tenantId(), definition.planId());
    }

    // tag::admit-effect-outcome-transaction[]
    @Transactional
    public ResolutionReceipt observe(EffectOutcomeSignal signal) {
        String fingerprint = fingerprint(signal);
        var prior = jdbc.query("""
                select plan_id, payload_sha256, disposition from resolution_message_inbox
                 where tenant_id=? and message_id=?
                """, (rs, row) -> new PriorMessage(
                        rs.getString(1), rs.getString(2), rs.getString(3)),
                signal.tenantId(), signal.messageId());
        if (!prior.isEmpty()) {
            if (!prior.get(0).planId().equals(signal.planId())) {
                throw new ResolutionMessageIdentityCollisionException(
                        "message identity is already bound to a different plan");
            }
            return currentWith(prior.get(0).payloadSha256().equals(fingerprint)
                    ? repeatedDisposition(prior.get(0).disposition())
                    : IDENTITY_COLLISION, signal.tenantId(), signal.planId());
        }
        var plan = requirePlan(signal.tenantId(), signal.planId(), true);
        var step = requireStep(signal.tenantId(), signal.planId(), signal.effectId());
        boolean earlierIncomplete = step.role().equals("FORWARD")
                && steps(signal.tenantId(), signal.planId()).stream()
                .anyMatch(candidate -> candidate.stepNumber() < step.stepNumber()
                        && !candidate.observedState().equals("SUCCEEDED"));
        if (earlierIncomplete) {
            inbox(signal, fingerprint, "OUT_OF_ORDER");
            return receipt(OUT_OF_ORDER, plan);
        }
        var snapshot = effects.snapshot(signal.tenantId(), signal.effectId());
        if (snapshot.version() < step.observedEffectVersion()
                || (snapshot.version() == step.observedEffectVersion()
                    && snapshot.state().name().equals(step.observedState()))) {
            inbox(signal, fingerprint, "STALE_OBSERVATION");
            return receipt(STALE_OBSERVATION, plan);
        }

        String ownedEvidenceRef = ownedEvidence(snapshot);
        jdbc.update("""
                update resolution_plan_effect
                   set observed_state=?, observed_effect_version=?, observation_evidence_ref=?
                 where tenant_id=? and plan_id=? and effect_id=?
                """, snapshot.state().name(), snapshot.version(), ownedEvidenceRef,
                signal.tenantId(), signal.planId(), signal.effectId());
        var next = decideState(signal.tenantId(), signal.planId());
        long nextVersion = plan.version() + 1;
        long eventNumber = plan.nextEventNumber();
        boolean completedCompensation = step.role().equals("COMPENSATION")
                && snapshot.state() == EffectReceipt.State.SUCCEEDED
                && next == COMPENSATED;
        int eventCount = completedCompensation ? 2 : 1;
        int updated = jdbc.update("""
                update resolution_plan set plan_state=?, version=?, next_event_number=?,
                       last_event_id=?, updated_at=?
                 where tenant_id=? and plan_id=? and version=?
                """, next.name(), nextVersion, eventNumber + eventCount,
                eventId(eventNumber + eventCount - 1), clock.instant(),
                signal.tenantId(), signal.planId(), plan.version());
        if (updated != 1) {
            throw new IllegalStateException("resolution plan changed concurrently");
        }
        inbox(signal, fingerprint, "APPLIED");
        afterState.afterStateMutation(signal.tenantId(), signal.planId(), next, nextVersion);
        var outcome = effectOutcomeDetails(step, snapshot, ownedEvidenceRef);
        outbox(signal.tenantId(), signal.planId(), eventNumber, nextVersion,
                completedCompensation ? "CompensationSucceeded"
                        : outcomeEventType(step.role(), snapshot.state(), next),
                next, clock.instant(), plan.lastEventId(), outcome);
        if (completedCompensation) {
            outbox(signal.tenantId(), signal.planId(), eventNumber + 1, nextVersion,
                    "ResolutionPlanCompensated", next, clock.instant(),
                    eventId(eventNumber), outcome);
        }
        return currentWith(APPLIED, signal.tenantId(), signal.planId());
    }
    // end::admit-effect-outcome-transaction[]

    // tag::select-governed-compensation[]
    @Transactional
    public ResolutionReceipt selectCompensation(SelectReservationReleaseRecovery command) {
        var plan = requirePlan(command.tenantId(), command.planId(), true);
        if (plan.state() != RECOVERY_DECISION_REQUIRED) {
            throw new IllegalStateException("plan is not eligible for automatic compensation");
        }
        if (!clock.instant().isBefore(plan.deadlineAt())) {
            throw new IllegalStateException("resolution plan deadline has passed");
        }
        var live = liveForwardStates(command.tenantId(), command.planId());
        if (live.stream().anyMatch(s -> s == EffectReceipt.State.UNKNOWN
                || s == EffectReceipt.State.ACCEPTED
                || s == EffectReceipt.State.DISPATCHING)) {
            throw new IllegalStateException("unsettled forward effects block compensation");
        }
        var failed = effects.snapshot(command.tenantId(), command.failedEffectId());
        var failedStep = requireStep(
                command.tenantId(), command.planId(), command.failedEffectId());
        if (failed.state() != EffectReceipt.State.FAILED_CONFIRMED) {
            throw new IllegalStateException("recovery cause is not a confirmed failure");
        }
        var compensation = command.compensation();
        var authority = compensation.authority();
        if (authority.planVersion() != plan.version()
                || !authority.failedEffectId().equals(command.failedEffectId())
                || authority.failedEffectVersion() != failed.version()
                || authority.failedEffectVersion() != failedStep.observedEffectVersion()) {
            throw new IllegalStateException(
                    "recovery authority does not bind the locked recovery premise");
        }
        if (!authority.evidenceSha256().equals(plan.evidenceSha256())
                || !authority.configurationRef().equals(plan.configurationRef())) {
            throw new IllegalStateException("recovery authority is stale for this plan");
        }
        var receipt = effects.register(compensation);
        if (receipt.disposition() == EffectReceipt.Disposition.IDENTITY_COLLISION) {
            throw new IllegalStateException("compensation effect identity collision");
        }
        jdbc.update("""
                insert into resolution_plan_effect
                (tenant_id, plan_id, step_number, effect_id, effect_role, effect_type,
                 caused_by_effect_id, compensates_effect_id, target_contract_ref,
                 authority_ref, authority_valid_until, evidence_sha256,
                 configuration_ref, reversibility, observed_state,
                 observed_effect_version, observation_evidence_ref)
                values (?,?,3,?,'COMPENSATION','RELEASE_INVENTORY_RESERVATION',?,?,?,?,?,?,?,
                        'CORRECTIVE_FORWARD_ONLY','RECORDED',0,'recovery-selected')
                """, command.tenantId(), command.planId(), compensation.effectId(),
                compensation.causedByEffectId(), compensation.compensatesEffectId(),
                compensation.targetContractRef(), authority.authorityId(), authority.validUntil(),
                authority.evidenceSha256(), authority.configurationRef());
        long nextVersion = plan.version() + 1;
        long firstEvent = plan.nextEventNumber();
        jdbc.update("""
                update resolution_plan set plan_state='COMPENSATION_PENDING', version=?,
                       next_event_number=?, last_event_id=?, updated_at=?
                 where tenant_id=? and plan_id=? and version=?
                """, nextVersion, firstEvent + 2, eventId(firstEvent + 1),
                clock.instant(), command.tenantId(),
                command.planId(), plan.version());
        afterState.afterStateMutation(command.tenantId(), command.planId(),
                COMPENSATION_PENDING, nextVersion);
        outbox(command.tenantId(), command.planId(), firstEvent, nextVersion,
                "RecoverySelected", COMPENSATION_PENDING, clock.instant(),
                plan.lastEventId(), Map.of(
                        "failedEffectId", command.failedEffectId(),
                        "failedEffectVersion", failed.version(),
                        "selectedAction", "RELEASE_INVENTORY_RESERVATION",
                        "effectId", compensation.effectId(),
                        "causedByEffectId", compensation.causedByEffectId(),
                        "compensatesEffectId", compensation.compensatesEffectId(),
                        "targetContractRef", compensation.targetContractRef()));
        outbox(command.tenantId(), command.planId(), firstEvent + 1, nextVersion,
                "RecoveryAuthorityIssued", COMPENSATION_PENDING, clock.instant(),
                eventId(firstEvent), Map.ofEntries(
                        Map.entry("effectId", compensation.effectId()),
                        Map.entry("compensatesEffectId", compensation.compensatesEffectId()),
                        Map.entry("authorityRef", authority.authorityId()),
                        Map.entry("authorityPlanVersion", authority.planVersion()),
                        Map.entry("failedEffectId", authority.failedEffectId()),
                        Map.entry("failedEffectVersion", authority.failedEffectVersion()),
                        Map.entry("policyRef", authority.policyRef()),
                        Map.entry("reasonCode", authority.reasonCode()),
                        Map.entry("evidenceSha256", authority.evidenceSha256()),
                        Map.entry("configurationRef", authority.configurationRef()),
                        Map.entry("authorityValidUntil", authority.validUntil())));
        return currentWith(APPLIED, command.tenantId(), command.planId());
    }
    // end::select-governed-compensation[]

    /** Explicitly reconsiders a retained out-of-order signal under a new message identity. */
    @Transactional
    public ResolutionReceipt redriveOutOfOrder(RedriveEffectOutcome command) {
        var prior = jdbc.query("""
                select plan_id, payload_sha256, disposition
                  from resolution_message_inbox
                 where tenant_id=? and message_id=?
                """, (rs, row) -> new PriorMessage(
                        rs.getString(1), rs.getString(2), rs.getString(3)),
                command.original().tenantId(), command.original().messageId());
        if (prior.isEmpty()
                || !prior.get(0).planId().equals(command.original().planId())
                || !prior.get(0).payloadSha256().equals(fingerprint(command.original()))
                || !prior.get(0).disposition().equals("OUT_OF_ORDER")) {
            throw new IllegalStateException(
                    "redrive does not reference an admitted out-of-order observation");
        }
        return observe(command.redrive());
    }

    /** Final dispatch admission for the one Chapter 16 compensation effect. */
    @Transactional(noRollbackFor = RecoveryDispatchRefusedException.class)
    public void requireCurrentRecoveryAuthority(String tenantId, String effectId) {
        var bindings = jdbc.query("""
                select plan_id, authority_ref, authority_valid_until,
                       compensates_effect_id, evidence_sha256, configuration_ref
                  from resolution_plan_effect
                 where tenant_id=? and effect_id=? and effect_role='COMPENSATION'
                """, (rs, row) -> new RecoveryBinding(
                        rs.getString(1), rs.getString(2),
                        rs.getObject(3, Instant.class), rs.getString(4),
                        rs.getString(5), rs.getString(6)), tenantId, effectId);
        if (bindings.isEmpty()) {
            throw new IllegalStateException("recovery effect is not bound to a resolution plan");
        }
        var binding = bindings.get(0);
        var plan = requirePlan(tenantId, binding.planId(), true);
        var intent = effects.snapshot(tenantId, effectId);
        if (plan.state() != COMPENSATION_PENDING) {
            // The plan already owns the refusal. Repeated dispatch attempts must not
            // create another state transition or another invalidation event.
            throw new RecoveryDispatchRefusedException("RECOVERY_PLAN_NOT_PENDING");
        }
        String refusal = null;
        if (binding.authorityValidUntil() == null
                || !clock.instant().isBefore(binding.authorityValidUntil())) {
            refusal = "RECOVERY_AUTHORITY_EXPIRED";
        } else if (!binding.authorityRef().equals(intent.authorityRef())
                || !binding.compensatesEffectId().equals(intent.compensatesEffectId())
                || !binding.evidenceSha256().equals(intent.authorityEvidenceSha256())
                || !binding.configurationRef().equals(intent.authorityConfigurationRef())) {
            refusal = "RECOVERY_AUTHORITY_BINDING_MISMATCH";
        }
        if (refusal == null) {
            return;
        }
        long nextVersion = plan.version() + 1;
        long eventNumber = plan.nextEventNumber();
        jdbc.update("""
                update resolution_plan set plan_state='MANUAL_RECOVERY', version=?,
                       next_event_number=?, last_event_id=?, updated_at=?
                 where tenant_id=? and plan_id=? and version=?
                """, nextVersion, eventNumber + 1, eventId(eventNumber),
                clock.instant(), tenantId,
                binding.planId(), plan.version());
        afterState.afterStateMutation(
                tenantId, binding.planId(), MANUAL_RECOVERY, nextVersion);
        outbox(tenantId, binding.planId(), eventNumber, nextVersion,
                "RecoveryAuthorityInvalidated", MANUAL_RECOVERY, clock.instant(),
                plan.lastEventId(), Map.of(
                        "effectId", effectId,
                        "compensatesEffectId", binding.compensatesEffectId(),
                        "authorityRef", binding.authorityRef(),
                        "reasonCode", refusal));
        throw new RecoveryDispatchRefusedException(refusal);
    }

    public ResolutionReceipt current(String tenantId, String planId) {
        return currentWith(APPLIED, tenantId, planId);
    }

    private ResolutionReceipt currentWith(
            ResolutionReceipt.Disposition disposition, String tenantId, String planId) {
        return receipt(disposition, requirePlan(tenantId, planId, false));
    }

    private ResolutionReceipt.State decideState(String tenantId, String planId) {
        var steps = steps(tenantId, planId);
        var compensation = steps.stream().filter(s -> s.role().equals("COMPENSATION")).findFirst();
        if (compensation.isPresent()) {
            return switch (EffectReceipt.State.valueOf(compensation.get().observedState())) {
                case SUCCEEDED -> COMPENSATED;
                case FAILED_CONFIRMED -> MANUAL_RECOVERY;
                case UNKNOWN, ACCEPTED, DISPATCHING -> OBSERVATION_REQUIRED;
                case RECORDED -> COMPENSATION_PENDING;
            };
        }
        if (steps.stream().anyMatch(s -> requiresObservation(s.observedState()))) {
            return OBSERVATION_REQUIRED;
        }
        if (steps.stream().anyMatch(s -> s.observedState().equals("RECORDED"))) {
            return FORWARD_RUNNING;
        }
        boolean allSucceeded = steps.stream().allMatch(s -> s.observedState().equals("SUCCEEDED"));
        if (allSucceeded) {
            return COMPLETED;
        }
        boolean priorSucceeded = steps.stream().anyMatch(s -> s.observedState().equals("SUCCEEDED"));
        boolean laterFailed = steps.stream().anyMatch(s -> s.observedState().equals("FAILED_CONFIRMED"));
        boolean compensatable = steps.stream().filter(s -> s.observedState().equals("SUCCEEDED"))
                .allMatch(s -> s.reversibility().equals("COMPENSATABLE"));
        return priorSucceeded && laterFailed && compensatable
                ? RECOVERY_DECISION_REQUIRED : MANUAL_RECOVERY;
    }

    private static boolean requiresObservation(String state) {
        return state.equals("UNKNOWN") || state.equals("ACCEPTED")
                || state.equals("DISPATCHING");
    }

    private List<EffectReceipt.State> liveForwardStates(String tenantId, String planId) {
        return steps(tenantId, planId).stream()
                .filter(s -> s.role().equals("FORWARD"))
                .map(s -> effects.snapshot(tenantId, s.effectId()).state()).toList();
    }

    private void validateDistinctOrderedEffects(ResolutionPlanDefinition definition) {
        var ids = definition.effects().stream().map(ResolutionPlanDefinition.ForwardEffect::effectId)
                .distinct().toList();
        if (ids.size() != definition.effects().size()) {
            throw new IllegalArgumentException("forward effect identities must be distinct");
        }
        for (int i = 0; i < definition.effects().size(); i++) {
            if (definition.effects().get(i).stepNumber() != i + 1) {
                throw new IllegalArgumentException("forward steps must be contiguous and ordered");
            }
        }
    }

    private void requirePlanEffect(
            ResolutionPlanDefinition plan, ResolutionPlanDefinition.ForwardEffect expected,
            JdbcEffectLedger.EffectSnapshot actual) {
        if (!plan.runId().equals(actual.runId()) || !plan.caseId().equals(actual.caseId())
                || !expected.effectType().equals(actual.effectType())
                || !expected.targetContractRef().equals(actual.targetContractRef())) {
            throw new IllegalArgumentException("plan effect differs from durable effect intent");
        }
        if (expected.stepNumber() > 1
                && !expected.causedByEffectId().equals(actual.causedByEffectId())) {
            throw new IllegalArgumentException("forward effect causation is unstable");
        }
        if (expected.stepNumber() > 1
                && !expected.authorityRef().equals(actual.authorityRef())) {
            throw new IllegalArgumentException("forward effect has different authority");
        }
        if (expected.stepNumber() > 1
                && !expected.authorityValidUntil().equals(actual.authorityValidUntil())) {
            throw new IllegalArgumentException("forward effect has different authority expiry");
        }
        if (expected.stepNumber() == 1) {
            var bound = jdbc.query("""
                    select authority_ref from authorized_effect
                     where tenant_id=? and effect_id=?
                    """, (rs, row) -> rs.getString(1), plan.tenantId(), expected.effectId());
            if (bound.isEmpty() || !bound.get(0).equals(expected.authorityRef())) {
                throw new IllegalArgumentException(
                        "first forward effect lacks its Chapter 15 authority binding");
            }
        }
    }

    private PlanRow requirePlan(String tenantId, String planId, boolean lock) {
        var rows = find(tenantId, planId, lock);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("unknown resolution plan");
        }
        return rows.get(0);
    }

    private List<PlanRow> find(String tenantId, String planId, boolean lock) {
        String sql = """
                select tenant_id, plan_id, plan_state, deadline_at, evidence_sha256,
                       configuration_ref, definition_sha256, next_event_number,
                       last_event_id, version
                  from resolution_plan where tenant_id=? and plan_id=?
                """ + (lock ? " for update" : "");
        return jdbc.query(sql, (rs, row) -> new PlanRow(
                rs.getString(1), rs.getString(2),
                ResolutionReceipt.State.valueOf(rs.getString(3)),
                rs.getObject(4, Instant.class), rs.getString(5), rs.getString(6),
                rs.getString(7), rs.getLong(8), rs.getString(9), rs.getLong(10)),
                tenantId, planId);
    }

    private StepRow requireStep(String tenantId, String planId, String effectId) {
        return steps(tenantId, planId).stream().filter(s -> s.effectId().equals(effectId))
                .findFirst().orElseThrow(() -> new IllegalArgumentException(
                        "effect is not part of the resolution plan"));
    }

    private List<StepRow> steps(String tenantId, String planId) {
        return jdbc.query("""
                select step_number, effect_id, effect_role, observed_state,
                       observed_effect_version, reversibility from resolution_plan_effect
                 where tenant_id=? and plan_id=? order by step_number
                """, (rs, row) -> new StepRow(rs.getInt(1), rs.getString(2), rs.getString(3),
                rs.getString(4), rs.getLong(5), rs.getString(6)), tenantId, planId);
    }

    private void inbox(EffectOutcomeSignal signal, String fingerprint, String disposition) {
        jdbc.update("""
                insert into resolution_message_inbox
                (tenant_id, message_id, plan_id, payload_sha256, disposition, received_at)
                values (?,?,?,?,?,?)
                """, signal.tenantId(), signal.messageId(), signal.planId(), fingerprint,
                disposition, clock.instant());
    }

    private void outbox(
            String tenantId, String planId, long eventNumber, long version,
            String eventType, ResolutionReceipt.State state, Instant createdAt,
            String causedByEventId, Map<String, ?> details) {
        String eventId = eventId(eventNumber);
        var payload = new LinkedHashMap<String, Object>();
        payload.put("eventId", eventId);
        payload.put("eventType", eventType);
        payload.put("causedByEventId", causedByEventId);
        payload.put("tenantId", tenantId);
        payload.put("planId", planId);
        payload.put("planVersion", version);
        payload.put("state", state.name());
        payload.putAll(details);
        jdbc.update("""
                insert into resolution_outbox
                (event_id, tenant_id, plan_id, plan_version, event_type,
                 event_payload, created_at, published_at)
                values (?,?,?,?,?,?,?,null)
                """, eventId, tenantId, planId,
                version, eventType, json(payload), createdAt);
    }

    private static String eventId(long eventNumber) {
        return "evt-" + String.format("%06d", eventNumber);
    }

    private Map<String, Object> effectOutcomeDetails(
            StepRow step, JdbcEffectLedger.EffectSnapshot snapshot,
            String ownedEvidenceRef) {
        var details = new LinkedHashMap<String, Object>();
        details.put("effectId", snapshot.effectId());
        details.put("effectRole", step.role());
        details.put("observedEffectState", snapshot.state().name());
        details.put("observedEffectVersion", snapshot.version());
        details.put("targetReference", snapshot.targetReference());
        details.put("targetEvidenceRef", ownedEvidenceRef);
        details.put("causedByEffectId", snapshot.causedByEffectId());
        details.put("compensatesEffectId", snapshot.compensatesEffectId());
        return details;
    }

    private String ownedEvidence(JdbcEffectLedger.EffectSnapshot snapshot) {
        if (snapshot.resolutionEvidenceRef() == null
                || snapshot.resolutionEvidenceRef().isBlank()) {
            throw new IllegalStateException(
                    "effect outcome has no ledger-owned evidence reference");
        }
        return snapshot.resolutionEvidenceRef();
    }

    private static ResolutionReceipt.Disposition repeatedDisposition(String disposition) {
        return switch (disposition) {
            case "OUT_OF_ORDER" -> OUT_OF_ORDER;
            case "STALE_OBSERVATION" -> STALE_OBSERVATION;
            default -> DUPLICATE_SAME;
        };
    }

    private static String outcomeEventType(
            String role, EffectReceipt.State effectState, ResolutionReceipt.State planState) {
        if (role.equals("COMPENSATION") && planState == COMPENSATED) {
            return "ResolutionPlanCompensated";
        }
        if (role.equals("COMPENSATION") && effectState == EffectReceipt.State.SUCCEEDED) {
            return "CompensationSucceeded";
        }
        return "ResolutionEffectObserved";
    }

    private ResolutionReceipt receipt(
            ResolutionReceipt.Disposition disposition, PlanRow row) {
        return new ResolutionReceipt(disposition, row.tenantId(), row.planId(),
                row.state(), row.version());
    }

    private String fingerprint(Object value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(mapper.writeValueAsBytes(value)));
        } catch (Exception failure) {
            throw new IllegalStateException("cannot fingerprint resolution input", failure);
        }
    }

    private String json(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception failure) {
            throw new IllegalStateException("cannot serialize resolution event", failure);
        }
    }

    private record PlanRow(
            String tenantId, String planId, ResolutionReceipt.State state,
            Instant deadlineAt, String evidenceSha256, String configurationRef,
            String definitionSha256, long nextEventNumber, String lastEventId,
            long version) { }

    private record StepRow(
            int stepNumber, String effectId, String role, String observedState,
            long observedEffectVersion, String reversibility) { }

    private record PriorMessage(
            String planId, String payloadSha256, String disposition) { }

    private record RecoveryBinding(
            String planId, String authorityRef, Instant authorityValidUntil,
            String compensatesEffectId, String evidenceSha256,
            String configurationRef) { }

    public static final class RecoveryDispatchRefusedException extends IllegalStateException {
        public RecoveryDispatchRefusedException(String reasonCode) {
            super("recovery dispatch refused: " + reasonCode.toLowerCase());
        }
    }

    public static final class ResolutionMessageIdentityCollisionException extends RuntimeException {
        public ResolutionMessageIdentityCollisionException(String message) { super(message); }
    }
}
