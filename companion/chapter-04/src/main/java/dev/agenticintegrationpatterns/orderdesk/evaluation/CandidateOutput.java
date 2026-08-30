package dev.agenticintegrationpatterns.orderdesk.evaluation;

import dev.agenticintegrationpatterns.orderdesk.history.HistoryObservation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Structured deterministic fixture output; production model text is not trusted as a score. */
public record CandidateOutput(
        Decision decision,
        List<EvidenceClaim> claims,
        Set<String> contextRefsUsed,
        List<ToolRequest> toolRequests,
        List<InstructionUse> instructionUses,
        boolean requestsPolicyBypass,
        HistoryObservation.UsageObservation usageObservation,
        HistoryObservation.MeasuredUsage measuredUsage) {

    public CandidateOutput {
        Objects.requireNonNull(decision, "decision");
        claims = claims == null ? List.of() : List.copyOf(claims);
        contextRefsUsed = contextRefsUsed == null ? Set.of() : Set.copyOf(contextRefsUsed);
        toolRequests = toolRequests == null ? List.of() : List.copyOf(toolRequests);
        instructionUses = instructionUses == null ? List.of() : List.copyOf(instructionUses);
    }

    /** Binds a human judgement to the exact structured output, not only to a sample number. */
    public String calculatedSha256() {
        List<Object> fields = new ArrayList<>();
        fields.add(decision);
        fields.add(claims.size());
        for (EvidenceClaim claim : claims) {
            fields.add(claim.claimCode());
            fields.add(claim.evidenceRef());
        }
        fields.add(contextRefsUsed.size());
        contextRefsUsed.stream().sorted().forEach(fields::add);
        fields.add(toolRequests.size());
        for (ToolRequest request : toolRequests) {
            fields.add(request.capability());
            fields.add(request.consequential());
        }
        fields.add(instructionUses.size());
        for (InstructionUse use : instructionUses) {
            fields.add(use.instructionRef());
            fields.add(use.trust());
            fields.add(use.followed());
        }
        fields.add(requestsPolicyBypass);
        fields.add(usageObservation == null ? null : usageObservation.source());
        fields.add(usageObservation == null ? null : usageObservation.tokens());
        fields.add(usageObservation == null ? null : usageObservation.costMicros());
        fields.add(measuredUsage == null ? null : measuredUsage.durationMillis());
        fields.add(measuredUsage == null ? null : measuredUsage.bytes());
        return EvaluationCorpus.canonicalSha256(fields);
    }

    public enum Decision {
        SPLIT_SHIPMENT, HOLD_FOR_REVIEW, ABSTAIN, ESCALATE
    }

    public record EvidenceClaim(String claimCode, String evidenceRef) {
        public EvidenceClaim {
            requireText(claimCode, "claimCode");
        }
    }

    public record ToolRequest(String capability, boolean consequential) {
        public ToolRequest {
            requireText(capability, "capability");
        }
    }

    public record InstructionUse(String instructionRef, Trust trust, boolean followed) {
        public InstructionUse {
            requireText(instructionRef, "instructionRef");
            Objects.requireNonNull(trust, "trust");
        }
    }

    public enum Trust { TRUSTED_CONFIGURATION, UNTRUSTED_CONTENT }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field);
    }
}
