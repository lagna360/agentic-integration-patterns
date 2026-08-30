package dev.agenticintegrationpatterns.orderdesk.effect;

import dev.agenticintegrationpatterns.orderdesk.recovery.JdbcResolutionRecoveryManager;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Clock;

import static dev.agenticintegrationpatterns.orderdesk.effect.InventoryReservationClient.ExternalOutcomeUnknownException;

@Component
public class EffectExecutionService {
    private final JdbcEffectLedger ledger;
    private final InventoryReservationClient client;
    private final SplitShipmentClient splitShipmentClient;
    private final InventoryReservationReleaseClient releaseClient;
    private final JdbcResolutionRecoveryManager recoveryManager;
    private final Clock clock;

    @Autowired
    public EffectExecutionService(
            JdbcEffectLedger ledger,
            InventoryReservationClient client,
            SplitShipmentClient splitShipmentClient,
            InventoryReservationReleaseClient releaseClient,
            JdbcResolutionRecoveryManager recoveryManager,
            Clock clock) {
        this.ledger = ledger;
        this.client = client;
        this.splitShipmentClient = splitShipmentClient;
        this.releaseClient = releaseClient;
        this.recoveryManager = recoveryManager;
        this.clock = clock;
    }

    /** Retained for the Chapter 13 malformed-adapter unit tests. */
    public EffectExecutionService(JdbcEffectLedger ledger, InventoryReservationClient client) {
        this(ledger, client, null, null, null, Clock.systemUTC());
    }

    // tag::one-effect-attempt[]
    public EffectReceipt executeOne(ExecuteEffect command) {
        var lease = ledger.claimRecorded(
                command.tenantId(), command.effectId(), command.workerId(),
                command.leaseDuration());
        if (lease.isEmpty()) {
            return ledger.current(command.tenantId(), command.effectId());
        }

        var claimed = lease.get();
        if (!claimed.effectType().equals("RESERVE_INVENTORY")) {
            throw new IllegalArgumentException("effect is not an inventory reservation");
        }
        var request = new InventoryReservationClient.ReservationRequest(
                claimed.tenantId(), claimed.effectId(), claimed.attemptId(),
                claimed.targetIdempotencyKey(), claimed.warehouseId(),
                claimed.sku(), claimed.quantity());
        try {
            return ledger.recordInvocation(claimed, client.reserve(request));
        } catch (ExternalOutcomeUnknownException unknown) {
            return ledger.recordUnknown(
                    claimed, "TARGET_REPLY_LOST", unknown.evidenceRef());
        }
    }
    // end::one-effect-attempt[]

    public EffectReceipt executeSplitShipment(ExecuteEffect command) {
        if (splitShipmentClient == null) {
            throw new IllegalStateException("split-shipment adapter is not configured");
        }
        var intent = ledger.snapshot(command.tenantId(), command.effectId());
        requireCurrentStoredAuthority(intent, "split-shipment");
        if (intent.causedByEffectId() == null
                || ledger.snapshot(command.tenantId(), intent.causedByEffectId()).state()
                    != EffectReceipt.State.SUCCEEDED) {
            throw new IllegalStateException(
                    "split shipment dispatch requires a confirmed reservation outcome");
        }
        var lease = ledger.claimRecorded(
                command.tenantId(), command.effectId(), command.workerId(),
                command.leaseDuration());
        if (lease.isEmpty()) {
            return ledger.current(command.tenantId(), command.effectId());
        }
        var claimed = lease.get();
        if (!claimed.effectType().equals("CREATE_SPLIT_SHIPMENT")) {
            throw new IllegalArgumentException("effect is not split-shipment creation");
        }
        try {
            return ledger.recordInvocation(claimed, splitShipmentClient.create(
                    new SplitShipmentClient.CreateRequest(
                        claimed.tenantId(), claimed.effectId(), claimed.attemptId(),
                        claimed.targetIdempotencyKey(), claimed.orderId(),
                        claimed.expectedOrderVersion(),
                        claimed.reservationReference(), claimed.warehouseId(),
                        claimed.sku(), claimed.quantity())));
        } catch (ExternalOutcomeUnknownException unknown) {
            return ledger.recordUnknown(
                    claimed, "TARGET_REPLY_LOST", unknown.evidenceRef());
        }
    }

    // tag::execute-compensation-as-effect[]
    public EffectReceipt executeRelease(ExecuteEffect command) {
        if (releaseClient == null) {
            throw new IllegalStateException("reservation-release adapter is not configured");
        }
        if (recoveryManager == null) {
            throw new IllegalStateException("recovery coordinator is not configured");
        }
        var intent = ledger.snapshot(command.tenantId(), command.effectId());
        if (intent.state() != EffectReceipt.State.RECORDED) {
            return ledger.current(command.tenantId(), command.effectId());
        }
        recoveryManager.requireCurrentRecoveryAuthority(
                command.tenantId(), command.effectId());
        requireCurrentStoredAuthority(intent, "recovery");
        var lease = ledger.claimRecorded(
                command.tenantId(), command.effectId(), command.workerId(),
                command.leaseDuration());
        if (lease.isEmpty()) {
            return ledger.current(command.tenantId(), command.effectId());
        }
        var claimed = lease.get();
        if (!claimed.effectType().equals("RELEASE_INVENTORY_RESERVATION")) {
            throw new IllegalArgumentException("effect is not a reservation release");
        }
        try {
            return ledger.recordInvocation(claimed, releaseClient.release(
                    new InventoryReservationReleaseClient.ReleaseRequest(
                        claimed.tenantId(), claimed.effectId(), claimed.attemptId(),
                        claimed.targetIdempotencyKey(), claimed.reservationReference(),
                        claimed.warehouseId(),
                        claimed.sku(), claimed.quantity())));
        } catch (ExternalOutcomeUnknownException unknown) {
            return ledger.recordUnknown(
                    claimed, "TARGET_REPLY_LOST", unknown.evidenceRef());
        }
    }
    // end::execute-compensation-as-effect[]

    private void requireCurrentStoredAuthority(
            JdbcEffectLedger.EffectSnapshot intent, String kind) {
        if (intent.authorityRef() == null || intent.authorityRef().isBlank()
                || intent.authorityValidUntil() == null
                || !clock.instant().isBefore(intent.authorityValidUntil())) {
            throw new IllegalStateException(kind + " authority is absent or expired");
        }
        if (kind.equals("recovery")
                && (intent.compensatesEffectId() == null
                    || intent.authorityEvidenceSha256() == null
                    || intent.authorityConfigurationRef() == null)) {
            throw new IllegalStateException("recovery authority binding is incomplete");
        }
    }

    // tag::target-reconciliation[]
    public EffectReceipt reconcile(ReconcileEffect command) {
        var lookup = ledger.targetLookup(command.tenantId(), command.effectId());
        var observation = client.findByIdempotencyKey(
                lookup.tenantId(), lookup.targetIdempotencyKey());
        return ledger.reconcile(
                command.tenantId(), command.effectId(), command.observationId(), observation);
    }
    // end::target-reconciliation[]
}
