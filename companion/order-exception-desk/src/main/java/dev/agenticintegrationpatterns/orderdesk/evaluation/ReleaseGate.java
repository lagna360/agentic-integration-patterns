package dev.agenticintegrationpatterns.orderdesk.evaluation;

import dev.agenticintegrationpatterns.orderdesk.evaluation.EvaluationRunner.BinomialEstimate;
import dev.agenticintegrationpatterns.orderdesk.evaluation.EvaluationRunner.EvaluationReport;
import dev.agenticintegrationpatterns.orderdesk.evaluation.EvaluationRunner.ScoreKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ReleaseGate {

    // tag::noncompensating-release-gate[]
    public ReleaseDecision decide(ReleasePolicy policy, EvaluationReport baseline,
            EvaluationReport candidate) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(candidate, "candidate");
        List<String> blockers = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        requirePinnedCorpus(policy, baseline, "BASELINE", blockers);
        requirePinnedCorpus(policy, candidate, "CANDIDATE", blockers);
        if (!policy.baselineCandidateRef().equals(baseline.candidate().candidateRef())
                || !policy.baselineCandidateSha256().equals(
                baseline.candidate().candidateSha256()))
            blockers.add("BASELINE_VERSION_NOT_PINNED");
        if (!policy.candidateRef().equals(candidate.candidate().candidateRef())
                || !policy.candidateSha256().equals(candidate.candidate().candidateSha256()))
            blockers.add("CANDIDATE_VERSION_NOT_PINNED");
        if (!baseline.complete()) blockers.add("BASELINE_CORPUS_INCOMPLETE");
        if (!candidate.complete()) blockers.add("CANDIDATE_CORPUS_INCOMPLETE");
        if (!candidate.blockingCandidateFindings().isEmpty())
            blockers.add("BLOCKING_CANDIDATE_FINDING");
        if (candidate.unresolvedHumanJudgements() > 0)
            blockers.add("HUMAN_JUDGEMENT_UNRESOLVED");
        if (candidate.unscorableSamples() > policy.maxUnscorableSamples())
            blockers.add("UNSCORABLE_LIMIT_EXCEEDED");
        else if (candidate.unscorableSamples() > 0)
            warnings.add("ACKNOWLEDGED_UNSCORABLE_SAMPLES=" + candidate.unscorableSamples());

        if (!policy.minimumLower95().keySet().equals(candidate.quality().keySet())
                || !policy.maximumPointRegression().keySet().equals(candidate.quality().keySet()))
            blockers.add("QUALITY_POLICY_COVERAGE_MISMATCH");
        policy.minimumLower95().forEach((scoreKey, minimum) -> {
            BinomialEstimate estimate = candidate.quality().get(scoreKey);
            if (estimate == null || estimate.lower95() < minimum)
                blockers.add("QUALITY_THRESHOLD_FAILED:" + scoreKey.externalName());
        });
        for (ScoreKey scoreKey : policy.maximumPointRegression().keySet()) {
            BinomialEstimate oldValue = baseline.quality().get(scoreKey);
            BinomialEstimate newValue = candidate.quality().get(scoreKey);
            if (oldValue == null || newValue == null) {
                blockers.add("BASELINE_COMPARISON_MISSING:" + scoreKey.externalName());
            } else if (oldValue.pointEstimate() - newValue.pointEstimate()
                    > policy.maximumPointRegression().get(scoreKey)) {
                blockers.add("REGRESSION_LIMIT_FAILED:" + scoreKey.externalName());
            }
        }
        return new ReleaseDecision(policy.policyRef(), policy.corpusId(), policy.corpusVersion(),
                policy.corpusSha256(), candidate.candidate().candidateRef(),
                candidate.candidate().candidateSha256(),
                blockers.isEmpty() ? Decision.ALLOW : Decision.BLOCK,
                policy.thresholdOwner(), List.copyOf(blockers), List.copyOf(warnings));
    }
    // end::noncompensating-release-gate[]

    private static void requirePinnedCorpus(ReleasePolicy policy, EvaluationReport report,
            String role, List<String> blockers) {
        if (!policy.corpusId().equals(report.corpusId())
                || !policy.corpusVersion().equals(report.corpusVersion())
                || !policy.corpusSha256().equals(report.corpusSha256()))
            blockers.add(role + "_CORPUS_NOT_PINNED");
    }

    public record ReleasePolicy(
            String policyRef,
            String thresholdOwner,
            String corpusId,
            String corpusVersion,
            String corpusSha256,
            String baselineCandidateRef,
            String baselineCandidateSha256,
            String candidateRef,
            String candidateSha256,
            Map<ScoreKey, Double> minimumLower95,
            Map<ScoreKey, Double> maximumPointRegression,
            int maxUnscorableSamples) {
        public ReleasePolicy {
            requireText(policyRef, "policyRef");
            requireText(thresholdOwner, "thresholdOwner");
            requireText(corpusId, "corpusId");
            requireText(corpusVersion, "corpusVersion");
            requireSha(corpusSha256, "corpusSha256");
            requireText(baselineCandidateRef, "baselineCandidateRef");
            requireSha(baselineCandidateSha256, "baselineCandidateSha256");
            requireText(candidateRef, "candidateRef");
            requireSha(candidateSha256, "candidateSha256");
            minimumLower95 = Map.copyOf(minimumLower95);
            maximumPointRegression = Map.copyOf(maximumPointRegression);
            if (minimumLower95.values().stream().anyMatch(value -> !Double.isFinite(value)
                    || value < 0 || value > 1))
                throw new IllegalArgumentException("minimumLower95");
            if (maximumPointRegression.values().stream().anyMatch(value -> !Double.isFinite(value)
                    || value < 0 || value > 1))
                throw new IllegalArgumentException("maximumPointRegression");
            if (maxUnscorableSamples < 0) throw new IllegalArgumentException("maxUnscorableSamples");
        }
    }

    public record ReleaseDecision(
            String policyRef,
            String corpusId,
            String corpusVersion,
            String corpusSha256,
            String candidateRef,
            String candidateSha256,
            Decision decision,
            String thresholdOwner,
            List<String> blockers,
            List<String> warnings) {
        public ReleaseDecision {
            requireText(policyRef, "policyRef");
            requireText(corpusId, "corpusId");
            requireText(corpusVersion, "corpusVersion");
            requireSha(corpusSha256, "corpusSha256");
            requireText(candidateRef, "candidateRef");
            requireSha(candidateSha256, "candidateSha256");
            Objects.requireNonNull(decision, "decision");
            requireText(thresholdOwner, "thresholdOwner");
            blockers = List.copyOf(blockers);
            warnings = List.copyOf(warnings);
            if (decision == Decision.ALLOW && !blockers.isEmpty())
                throw new IllegalArgumentException("allow decision has blockers");
            if (decision == Decision.BLOCK && blockers.isEmpty())
                throw new IllegalArgumentException("blocked decision lacks reason");
        }
    }

    public enum Decision { ALLOW, BLOCK }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field);
    }

    private static void requireSha(String value, String field) {
        if (value == null || !value.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException(field);
    }
}
