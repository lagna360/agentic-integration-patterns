package dev.agenticintegrationpatterns.orderdesk.routing;

import dev.agenticintegrationpatterns.orderdesk.capability.CapabilityEvidence;
import dev.agenticintegrationpatterns.orderdesk.context.ResolvedInvestigationContext;

import java.util.List;

public record InvestigationRoutingRequest(
        ResolvedInvestigationContext context,
        List<CapabilityEvidence> capabilityEvidence,
        AdvisoryRoutingAssessment assessment) {

    public InvestigationRoutingRequest {
        capabilityEvidence = capabilityEvidence == null
                ? List.of() : List.copyOf(capabilityEvidence);
    }
}
