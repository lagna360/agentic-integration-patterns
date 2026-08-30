package dev.agenticintegrationpatterns.orderdesk.coordination;

import dev.agenticintegrationpatterns.orderdesk.context.AdmittedWorkFingerprint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.EnumSet;
import java.util.UUID;

import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationBranch.INVENTORY_RECHECK;
import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationBranch.ORDER_HISTORY;

@Component
public final class ParallelInvestigationPlanProvider {
    public static final String PLAN_VERSION = "orderdesk-parallel-investigation-v1";

    private final Clock clock;
    private final ObjectMapper mapper;
    private final long timeoutMillis;

    public ParallelInvestigationPlanProvider(
            Clock clock,
            ObjectMapper mapper,
            @Value("${orderdesk.parallel.timeout-ms:250}") long timeoutMillis) {
        this.clock = clock;
        this.mapper = mapper;
        this.timeoutMillis = timeoutMillis;
    }

    public ParallelInvestigationPlan plan(ParallelInvestigationRequest request) {
        if (request == null || request.context() == null
                || request.context().admitted() == null
                || request.context().snapshot() == null
                || request.context().modelContext() == null) {
            throw new IllegalArgumentException("Resolved investigation context is required");
        }
        var context = request.context();
        var admitted = context.admitted();
        var command = admitted.command();
        var snapshot = context.snapshot();
        if (!snapshot.tenantId().equals(command.tenantId())
                || !snapshot.tenantId().equals(admitted.trustedContext().tenantId())
                || !snapshot.snapshotId().equals(context.modelContext().snapshotId())
                || !snapshot.admittedWorkFingerprint().equals(
                        AdmittedWorkFingerprint.compute(mapper, admitted))) {
            throw new IllegalArgumentException("Run, tenant, and retained context are inconsistent");
        }
        if (timeoutMillis <= 0 || command.deadlineAt() == null
                || !command.deadlineAt().isAfter(clock.instant().plusMillis(timeoutMillis))) {
            throw new IllegalArgumentException(
                    "The run deadline cannot admit the configured scatter window");
        }

        var expected = EnumSet.noneOf(InvestigationBranch.class);
        var required = EnumSet.noneOf(InvestigationBranch.class);
        if (admitted.effectiveCapabilities().contains("read-inventory")) {
            expected.add(INVENTORY_RECHECK);
            required.add(INVENTORY_RECHECK);
        }
        if (admitted.effectiveCapabilities().contains("read-order")) {
            expected.add(ORDER_HISTORY);
        }
        if (!required.contains(INVENTORY_RECHECK)) {
            throw new IllegalArgumentException(
                    "This investigation plan requires the read-inventory grant");
        }

        String material = snapshot.tenantId() + "|" + snapshot.runId() + "|" + PLAN_VERSION;
        String scatterId = "scatter-" + UUID.nameUUIDFromBytes(
                material.getBytes(StandardCharsets.UTF_8));
        return new ParallelInvestigationPlan(
                scatterId, PLAN_VERSION, snapshot.runId(), snapshot.tenantId(),
                command.deadlineAt(), expected, required, true);
    }
}
