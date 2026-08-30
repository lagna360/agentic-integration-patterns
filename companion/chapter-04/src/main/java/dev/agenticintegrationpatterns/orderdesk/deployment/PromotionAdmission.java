package dev.agenticintegrationpatterns.orderdesk.deployment;

import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.Stage;
import dev.agenticintegrationpatterns.orderdesk.evaluation.ReleaseGate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A stateless admission check for one post-evaluation stage transition. Development and
 * evaluation create release evidence and are deliberately outside this check. Passing it does
 * not perform a deployment, grant access to a later stage, or authorize a business effect.
 */
public final class PromotionAdmission {
    private final ProtectedPromotionRecords protectedRecords;

    public PromotionAdmission(ProtectedPromotionRecords protectedRecords) {
        this.protectedRecords = Objects.requireNonNull(protectedRecords, "protectedRecords");
    }

    // tag::bounded-stage-promotion[]
    public AdmissionResult assess(String releaseDecisionRef, Stage currentStage,
            DeploymentBoundaryManifest target, String authorityRef, Instant now) {
        requireText(releaseDecisionRef, "releaseDecisionRef");
        Objects.requireNonNull(currentStage, "currentStage");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(now, "now");
        List<String> blockers = new ArrayList<>();
        ReleaseEvidence releaseEvidence = protectedRecords.releaseEvidence(releaseDecisionRef)
                .orElse(null);
        PromotionAuthority authority = authorityRef == null ? null
                : protectedRecords.promotionAuthority(authorityRef).orElse(null);

        if (target.stage().ordinal() < Stage.STAGING.ordinal())
            blockers.add("PRE_RELEASE_TRANSITION_OUT_OF_SCOPE:" + target.stage());
        if (releaseEvidence == null) {
            blockers.add("RELEASE_EVIDENCE_MISSING:" + releaseDecisionRef);
        } else {
            if (releaseEvidence.decision().decision() != ReleaseGate.Decision.ALLOW)
                blockers.add("RELEASE_DECISION_BLOCKED:" + releaseEvidence.releaseDecisionRef());
            if (releaseEvidence.approvedStage() != target.stage())
                blockers.add("RELEASE_DECISION_STAGE_MISMATCH");
            if (!releaseEvidence.candidateRef().equals(target.candidateRef())
                    || !releaseEvidence.candidateSha256().equals(target.candidateSha256()))
                blockers.add("RELEASE_CANDIDATE_NOT_PINNED");
        }
        if (!isNextStage(currentStage, target.stage()))
            blockers.add("NON_SEQUENTIAL_STAGE_TRANSITION:" + currentStage + "->"
                    + target.stage());

        if (authority == null) {
            blockers.add("STAGE_AUTHORITY_MISSING:" + target.stage());
        } else {
            if (!authority.releaseDecisionRef().equals(releaseDecisionRef))
                blockers.add("STAGE_AUTHORITY_RELEASE_MISMATCH");
            if (!authority.candidateRef().equals(target.candidateRef())
                    || !authority.candidateSha256().equals(target.candidateSha256()))
                blockers.add("STAGE_AUTHORITY_CANDIDATE_MISMATCH");
            if (authority.targetStage() != target.stage())
                blockers.add("STAGE_AUTHORITY_TARGET_MISMATCH");
            if (!authority.deploymentManifestSha256().equals(target.manifestSha256()))
                blockers.add("STAGE_AUTHORITY_MANIFEST_MISMATCH");
            if (now.isBefore(authority.issuedAt()) || !now.isBefore(authority.expiresAt()))
                blockers.add("STAGE_AUTHORITY_NOT_CURRENT");
            if (authority.revokedAt() != null && !now.isBefore(authority.revokedAt()))
                blockers.add("STAGE_AUTHORITY_REVOKED");
            if (authority.supersededBy() != null)
                blockers.add("STAGE_AUTHORITY_SUPERSEDED:" + authority.supersededBy());
        }

        return new AdmissionResult(blockers.isEmpty() ? Decision.ALLOW : Decision.BLOCK,
                currentStage, target.stage(), List.copyOf(blockers),
                "Admission applies only to this exact candidate, manifest, and stage; "
                        + "it grants neither a later-stage promotion nor business-effect authority.");
    }
    // end::bounded-stage-promotion[]

    private static boolean isNextStage(Stage current, Stage target) {
        return switch (current) {
            case UNDEPLOYED, DEVELOPMENT -> false;
            case EVALUATION -> target == Stage.STAGING;
            case STAGING -> target == Stage.CANARY;
            case CANARY -> target == Stage.PRODUCTION;
            case PRODUCTION -> false;
        };
    }

    public record ReleaseEvidence(String releaseDecisionRef, String evaluationRunRef,
            Stage approvedStage, ReleaseGate.ReleaseDecision decision) {
        public ReleaseEvidence {
            requireText(releaseDecisionRef, "releaseDecisionRef");
            requireText(evaluationRunRef, "evaluationRunRef");
            Objects.requireNonNull(approvedStage, "approvedStage");
            if (approvedStage.ordinal() < Stage.STAGING.ordinal())
                throw new IllegalArgumentException("approvedStage");
            Objects.requireNonNull(decision, "decision");
        }

        public String candidateRef() { return decision.candidateRef(); }
        public String candidateSha256() { return decision.candidateSha256(); }
    }

    public record PromotionAuthority(String authorityRef, String issuerRef,
            String authorityPolicyRef, String approverRole,
            String releaseDecisionRef, String candidateRef, String candidateSha256,
            Stage targetStage, String deploymentManifestSha256, Instant issuedAt,
            Instant expiresAt, Instant revokedAt, String supersededBy) {
        public PromotionAuthority {
            requireText(authorityRef, "authorityRef");
            requireText(issuerRef, "issuerRef");
            requireText(authorityPolicyRef, "authorityPolicyRef");
            requireText(approverRole, "approverRole");
            requireText(releaseDecisionRef, "releaseDecisionRef");
            requireText(candidateRef, "candidateRef");
            requireSha(candidateSha256, "candidateSha256");
            Objects.requireNonNull(targetStage, "targetStage");
            if (targetStage.ordinal() < Stage.STAGING.ordinal())
                throw new IllegalArgumentException("targetStage");
            requireSha(deploymentManifestSha256, "deploymentManifestSha256");
            Objects.requireNonNull(issuedAt, "issuedAt");
            Objects.requireNonNull(expiresAt, "expiresAt");
            if (!expiresAt.isAfter(issuedAt)) throw new IllegalArgumentException("expiresAt");
            if (revokedAt != null && revokedAt.isBefore(issuedAt))
                throw new IllegalArgumentException("revokedAt");
            if (supersededBy != null) requireText(supersededBy, "supersededBy");
        }
    }

    /**
     * Loads immutable release and authority records from protected storage. Implementations must
     * authenticate that storage, authorize the caller, validate record integrity, and exclude
     * caller-supplied record bodies; the strings passed to {@link #assess} are only references.
     */
    public interface ProtectedPromotionRecords {
        Optional<ReleaseEvidence> releaseEvidence(String releaseDecisionRef);
        Optional<PromotionAuthority> promotionAuthority(String authorityRef);
    }

    public record AdmissionResult(Decision decision, Stage currentStage, Stage targetStage,
            List<String> blockers, String qualification) {
        public AdmissionResult {
            Objects.requireNonNull(decision, "decision");
            Objects.requireNonNull(currentStage, "currentStage");
            Objects.requireNonNull(targetStage, "targetStage");
            blockers = List.copyOf(blockers);
            requireText(qualification, "qualification");
        }
    }

    public enum Decision { ALLOW, BLOCK }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field);
        return value;
    }

    private static String requireSha(String value, String field) {
        if (value == null || !value.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException(field);
        return value;
    }
}
