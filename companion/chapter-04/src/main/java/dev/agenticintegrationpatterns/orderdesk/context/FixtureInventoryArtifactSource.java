package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Component
public final class FixtureInventoryArtifactSource implements ArtifactSource {
    private static final String REFERENCE =
            "inventory://yyz-01/camera-battery-x2@740";

    @Override
    public boolean supports(String sourceSystem) {
        return "inventory-ledger".equals(sourceSystem);
    }

    @Override
    public SourceArtifact acquire(
            String tenantId,
            InvestigateOrderException.EvidenceReference reference) {
        if (!"tenant-ca".equals(tenantId) || !REFERENCE.equals(reference.reference())) {
            return null;
        }
        byte[] content = """
                {"sku":"camera-battery-x2","warehouse":"yyz-01","available":0,"requested":2}
                """.strip().getBytes(StandardCharsets.UTF_8);
        return new SourceArtifact(
                tenantId,
                REFERENCE,
                "inventory-ledger",
                "740",
                Instant.parse("2026-08-24T06:13:12Z"),
                Instant.parse("2026-08-24T06:18:12Z"),
                "application/json",
                InvestigateOrderException.EvidenceTrust.AUTHORITATIVE_SOURCE,
                content);
    }
}
