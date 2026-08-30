package dev.agenticintegrationpatterns.orderdesk.coordination;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.UUID;

import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationBranch.INVENTORY_RECHECK;
import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationReply.Status.SUCCEEDED;

@Component
public final class FixtureParallelInvestigator implements ParallelInvestigator {
    private final Clock clock;

    public FixtureParallelInvestigator(Clock clock) {
        this.clock = clock;
    }

    @Override
    public InvestigationReply investigate(
            InvestigationBranch branch,
            ParallelBranchRequest request) {
        var now = clock.instant();
        var finding = switch (branch) {
            case INVENTORY_RECHECK -> finding(
                    "inventory.availableUnits:yyz-01:camera-battery-x2", "0",
                    "inventory-ledger", "740", now);
            case ORDER_HISTORY -> finding(
                    "order.fulfillmentState:order-100045", "BACKORDERED",
                    "order-service", "17", now);
        };
        String replyMaterial = request.plan().scatterId() + "|" + branch;
        return new InvestigationReply(
                "reply-" + UUID.nameUUIDFromBytes(
                        replyMaterial.getBytes(StandardCharsets.UTF_8)),
                request.plan().scatterId(), request.plan().runId(), request.plan().tenantId(),
                branch, SUCCEEDED, finding, now);
    }

    private static EvidenceFinding finding(
            String key,
            String value,
            String source,
            String version,
            java.time.Instant observedAt) {
        return new EvidenceFinding(
                key, value, ParallelEvidenceDigests.sha256(value),
                source, version, observedAt);
    }
}
