package dev.agenticintegrationpatterns.orderdesk.approval;

import dev.agenticintegrationpatterns.orderdesk.effect.EffectExecutionService;
import dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt;
import dev.agenticintegrationpatterns.orderdesk.effect.ExecuteEffect;
import dev.agenticintegrationpatterns.orderdesk.effect.JdbcEffectLedger;
import dev.agenticintegrationpatterns.orderdesk.effect.ReserveInventoryEffect;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/** Guarded Chapter 15 seam; the lower-level Chapter 13 service keeps its explicit precondition. */
@Component
public class GuardedEffectService {
    private final JdbcApprovalService approvals;
    private final JdbcEffectLedger ledger;
    private final EffectExecutionService execution;
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public GuardedEffectService(
            JdbcApprovalService approvals, JdbcEffectLedger ledger,
            EffectExecutionService execution, JdbcTemplate jdbc, Clock clock) {
        this.approvals = approvals;
        this.ledger = ledger;
        this.execution = execution;
        this.jdbc = jdbc;
        this.clock = clock;
    }

    // tag::authorized-effect-registration[]
    @Transactional
    public EffectReceipt registerAuthorized(RegisterAuthorizedEffect command) {
        var subject = command.subject();
        var effect = command.effect();
        requireExactEffect(subject, effect);
        var authority = approvals.requireCurrentAuthority(
                subject.tenantId(), command.requestId(), subject);
        var receipt = ledger.register(effect);
        if (receipt.disposition() == EffectReceipt.Disposition.IDENTITY_COLLISION) {
            throw new EffectAuthorityDeniedException("effect identity collides with existing intent");
        }
        var existing = jdbc.query("""
                select request_id, subject_sha256 from authorized_effect
                 where tenant_id=? and effect_id=?
                """, (rs, n) -> java.util.Map.entry(rs.getString(1), rs.getString(2)),
                subject.tenantId(), subject.effectId());
        if (existing.isEmpty()) {
            jdbc.update("""
                    insert into authorized_effect
                    (tenant_id, effect_id, request_id, approval_version, subject_sha256,
                     authority_ref, recorded_at) values (?,?,?,?,?,?,?)
                    """, subject.tenantId(), subject.effectId(), command.requestId(),
                    authority.version(), authority.subjectSha256(), authority.authorityRef(),
                    clock.instant());
        } else if (!existing.get(0).getKey().equals(command.requestId())
                || !existing.get(0).getValue().equals(authority.subjectSha256())) {
            throw new EffectAuthorityDeniedException("effect is bound to different authority");
        }
        return receipt;
    }
    // end::authorized-effect-registration[]

    // tag::current-authority-before-dispatch[]
    // tag::current-authority-gate[]
    public EffectReceipt executeAuthorized(ExecuteEffect command) {
        var bindings = jdbc.query("""
                select request_id, subject_sha256, approval_version from authorized_effect
                 where tenant_id=? and effect_id=?
                """, (rs, n) -> new AuthorityBinding(
                        rs.getString(1), rs.getString(2), rs.getLong(3)),
                command.tenantId(), command.effectId());
        if (bindings.isEmpty()) {
            throw new EffectAuthorityDeniedException("effect has no recorded authority");
        }
        var binding = bindings.get(0);
        approvals.requireCurrentAuthority(command.tenantId(), binding.requestId(),
                command.effectId(), binding.subjectSha256(), binding.approvalVersion());
        return execution.executeOne(command);
    }
    // end::current-authority-gate[]
    // end::current-authority-before-dispatch[]

    private void requireExactEffect(ApprovalSubject subject, ReserveInventoryEffect effect) {
        if (!subject.tenantId().equals(effect.tenantId())
                || !subject.runId().equals(effect.runId())
                || !subject.caseId().equals(effect.caseId())
                || !subject.effectId().equals(effect.effectId())
                || !subject.warehouseId().equals(effect.warehouseId())
                || !subject.sku().equals(effect.sku())
                || subject.quantity() != effect.quantity()
                || !subject.effectIntentSha256().equals(ledger.intentSha256(effect))) {
            throw new EffectAuthorityDeniedException("effect parameters differ from approved subject");
        }
    }

    private record AuthorityBinding(
            String requestId, String subjectSha256, long approvalVersion) { }
}
