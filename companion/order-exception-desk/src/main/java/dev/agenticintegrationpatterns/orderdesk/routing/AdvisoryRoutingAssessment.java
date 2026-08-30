package dev.agenticintegrationpatterns.orderdesk.routing;

import java.util.List;

public record AdvisoryRoutingAssessment(
        String recommendedClass,
        double supportScore,
        List<String> evidenceIds,
        String rationale) {

    public AdvisoryRoutingAssessment {
        evidenceIds = evidenceIds == null ? null : List.copyOf(evidenceIds);
    }
}
