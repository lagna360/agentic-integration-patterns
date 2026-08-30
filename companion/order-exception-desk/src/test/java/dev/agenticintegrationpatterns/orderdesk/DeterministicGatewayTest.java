package dev.agenticintegrationpatterns.orderdesk;

import dev.agenticintegrationpatterns.orderdesk.application.DeterministicFailureAssessmentGateway;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentDisposition;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import dev.agenticintegrationpatterns.orderdesk.model.CaseWork;
import dev.agenticintegrationpatterns.orderdesk.model.EvidenceReference;
import dev.agenticintegrationpatterns.orderdesk.model.InventoryObservation;
import dev.agenticintegrationpatterns.orderdesk.model.InventoryShortfallDetected;
import dev.agenticintegrationpatterns.orderdesk.model.OrderContext;
import dev.agenticintegrationpatterns.orderdesk.model.OrderExceptionCase;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DeterministicGatewayTest {
    @Test
    void fixtureGatewayProducesARepeatableAssessment() {
        Instant observedAt = Instant.parse("2026-08-24T06:13:12Z");
        String reference = "inventory://yyz-02/sku@7";
        var event = new InventoryShortfallDetected(1, "evt-1", "corr-1", "tenant-1",
                "InventoryShortfallDetected", observedAt, "order-1", "sku", 2, 0, "yyz-01");
        var request = new AssessmentRequest(
                new CaseWork(event, new OrderExceptionCase(
                        "case-1", "tenant-1", "order-1", "sku", 1, "OPEN")),
                new OrderContext("order-1",
                        List.of(new InventoryObservation("yyz-02", 2, observedAt, reference)),
                        List.of(new EvidenceReference(reference, observedAt, true))));

        var result = new DeterministicFailureAssessmentGateway().assess(request);

        assertThat(result.assessment().disposition()).isEqualTo(AssessmentDisposition.PROPOSE_RESOLUTION);
        assertThat(result.assessment().evidenceReferences()).containsExactly(reference);
        assertThat(result.provenance().provider()).isEqualTo("fixture");
    }
}
