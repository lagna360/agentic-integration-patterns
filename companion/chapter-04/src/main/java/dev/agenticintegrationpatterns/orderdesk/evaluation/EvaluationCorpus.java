package dev.agenticintegrationpatterns.orderdesk.evaluation;

import dev.agenticintegrationpatterns.orderdesk.history.ReplayInputManifest;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** A small executable example of a versioned corpus, not a general evaluation format. */
// tag::versioned-evaluation-corpus[]
public final class EvaluationCorpus {
    private final String tenantId;
    private final String corpusId;
    private final String version;
    private final Instant frozenAt;
    private final List<EvaluationCase> cases;
    private final String manifestSha256;

    private EvaluationCorpus(String tenantId, String corpusId, String version, Instant frozenAt,
            List<EvaluationCase> cases, String claimedSha256) {
        this.tenantId = requireText(tenantId, "tenantId");
        this.corpusId = requireText(corpusId, "corpusId");
        this.version = requireText(version, "version");
        this.frozenAt = Objects.requireNonNull(frozenAt, "frozenAt");
        this.cases = List.copyOf(cases);
        if (this.cases.isEmpty()) throw new IllegalArgumentException("cases");
        if (new HashSet<>(this.cases.stream().map(EvaluationCase::key).toList()).size()
                != this.cases.size()) throw new IllegalArgumentException("duplicate case key");
        if (this.cases.stream().anyMatch(item -> !tenantId.equals(item.retainedInput().tenantId())))
            throw new IllegalArgumentException("case tenant mismatch");
        String calculated = calculatedSha256();
        if (claimedSha256 != null && !calculated.equals(claimedSha256))
            throw new IllegalArgumentException("corpus manifest digest mismatch");
        this.manifestSha256 = calculated;
    }

    public static EvaluationCorpus seal(String tenantId, String corpusId, String version,
            Instant frozenAt, List<EvaluationCase> cases) {
        return new EvaluationCorpus(tenantId, corpusId, version, frozenAt, cases, null);
    }

    public static EvaluationCorpus verify(String tenantId, String corpusId, String version,
            Instant frozenAt, List<EvaluationCase> cases, String manifestSha256) {
        return new EvaluationCorpus(tenantId, corpusId, version, frozenAt, cases,
                requireSha(manifestSha256, "manifestSha256"));
    }

    public String tenantId() { return tenantId; }
    public String corpusId() { return corpusId; }
    public String version() { return version; }
    public Instant frozenAt() { return frozenAt; }
    public List<EvaluationCase> cases() { return cases; }
    public String manifestSha256() { return manifestSha256; }

    public String calculatedSha256() {
        List<Object> fields = new ArrayList<>(List.of(tenantId, corpusId, version, frozenAt));
        fields.add(cases.size());
        cases.stream().sorted(Comparator.comparing(EvaluationCase::key)).forEach(item -> {
            fields.add(item.caseId());
            fields.add(item.caseVersion());
            fields.add(item.scenario());
            fields.add(item.retainedInput().manifestSha256());
            fields.add(item.retainedInput().retentionState());
            fields.add(item.oracle().descriptor());
            fields.add(item.repetitions());
            fields.add(item.allowedEvidenceRefs().size());
            item.allowedEvidenceRefs().stream().sorted().forEach(fields::add);
        });
        return canonicalSha256(fields);
    }

    // tag::versioned-evaluation-case[]
    public record EvaluationCase(
            String caseId,
            String caseVersion,
            Scenario scenario,
            ReplayInputManifest retainedInput,
            Oracle oracle,
            Set<String> allowedEvidenceRefs,
            int repetitions) {

        public EvaluationCase {
            requireText(caseId, "caseId");
            requireText(caseVersion, "caseVersion");
            if (caseId.contains("@") || caseVersion.contains("@"))
                throw new IllegalArgumentException("case key separator");
            Objects.requireNonNull(scenario, "scenario");
            Objects.requireNonNull(retainedInput, "retainedInput");
            Objects.requireNonNull(oracle, "oracle");
            allowedEvidenceRefs = allowedEvidenceRefs == null
                    ? Set.of() : Set.copyOf(allowedEvidenceRefs);
            if (repetitions < 1 || repetitions > 100) throw new IllegalArgumentException("repetitions");
            if (!retainedInput.calculatedSha256().equals(retainedInput.manifestSha256()))
                throw new IllegalArgumentException("retained input digest mismatch");
            if (!"RETAINED".equals(retainedInput.retentionState()))
                throw new IllegalArgumentException("retained input unavailable");
        }

        public String key() { return caseId + "@" + caseVersion; }
    }
    // end::versioned-evaluation-case[]

    public enum Scenario {
        GOLDEN_PATH, PARTIAL_EVIDENCE, CONFLICTING_EVIDENCE, PROMPT_INJECTION,
        PROVIDER_DRIFT, HUMAN_JUDGMENT, UNSCORABLE_MISSING_PROVENANCE
    }

    public sealed interface Oracle permits ExactOracle, AdmissibleSetOracle,
            PropertyOracle, HumanRequiredOracle, UnscorableOracle {
        OracleAssessment assess(CandidateOutput output, HumanJudgement judgement);
        String descriptor();
    }

    public record ExactOracle(CandidateOutput.Decision expected) implements Oracle {
        public ExactOracle { Objects.requireNonNull(expected, "expected"); }
        @Override public OracleAssessment assess(CandidateOutput output, HumanJudgement ignored) {
            return outcome(output.decision() == expected, "EXACT_DECISION");
        }
        @Override public String descriptor() { return "EXACT:" + expected; }
    }

    public record AdmissibleSetOracle(Set<CandidateOutput.Decision> admitted) implements Oracle {
        public AdmissibleSetOracle {
            admitted = Set.copyOf(admitted);
            if (admitted.isEmpty()) throw new IllegalArgumentException("admitted");
        }
        @Override public OracleAssessment assess(CandidateOutput output, HumanJudgement ignored) {
            return outcome(admitted.contains(output.decision()), "ADMISSIBLE_SET");
        }
        @Override public String descriptor() {
            return "ADMISSIBLE:" + admitted.stream().map(Enum::name).sorted().toList();
        }
    }

    public record PropertyOracle(Property property) implements Oracle {
        public PropertyOracle { Objects.requireNonNull(property, "property"); }
        @Override public OracleAssessment assess(CandidateOutput output, HumanJudgement ignored) {
            return outcome(output.decision() == property.requiredDecision(), property.name());
        }
        @Override public String descriptor() { return "PROPERTY:" + property; }
    }

    public enum Property {
        ABSTAIN_ON_PARTIAL_EVIDENCE(CandidateOutput.Decision.ABSTAIN),
        ESCALATE_ON_CONFLICT(CandidateOutput.Decision.ESCALATE),
        ABSTAIN_ON_PROVIDER_DRIFT(CandidateOutput.Decision.ABSTAIN);

        private final CandidateOutput.Decision requiredDecision;
        Property(CandidateOutput.Decision requiredDecision) {
            this.requiredDecision = requiredDecision;
        }
        CandidateOutput.Decision requiredDecision() { return requiredDecision; }
    }

    public record HumanRequiredOracle(String rubricRef) implements Oracle {
        public HumanRequiredOracle { requireText(rubricRef, "rubricRef"); }
        @Override public OracleAssessment assess(CandidateOutput output, HumanJudgement judgement) {
            if (judgement == null) return new OracleAssessment(OracleStatus.HUMAN_REQUIRED,
                    "HUMAN_JUDGEMENT_MISSING");
            if (!rubricRef.equals(judgement.rubricRef())) return new OracleAssessment(
                    OracleStatus.HUMAN_REQUIRED, "HUMAN_RUBRIC_MISMATCH");
            return outcome(judgement.accepted(), judgement.reasonCode());
        }
        @Override public String descriptor() { return "HUMAN:" + rubricRef; }
    }

    public record UnscorableOracle(String reasonCode) implements Oracle {
        public UnscorableOracle { requireText(reasonCode, "reasonCode"); }
        @Override public OracleAssessment assess(CandidateOutput output, HumanJudgement ignored) {
            return new OracleAssessment(OracleStatus.UNSCORABLE, reasonCode);
        }
        @Override public String descriptor() { return "UNSCORABLE:" + reasonCode; }
    }

    public record HumanJudgement(
            boolean accepted,
            String candidateSha256,
            String outputSha256,
            String caseKey,
            int sampleNumber,
            String rubricRef,
            String reviewerRole,
            String reasonCode) {
        public HumanJudgement {
            requireSha(candidateSha256, "candidateSha256");
            requireSha(outputSha256, "outputSha256");
            requireText(caseKey, "caseKey");
            if (sampleNumber < 1) throw new IllegalArgumentException("sampleNumber");
            requireText(rubricRef, "rubricRef");
            requireText(reviewerRole, "reviewerRole");
            requireText(reasonCode, "reasonCode");
        }

        boolean binds(String expectedCandidateSha256, String expectedOutputSha256,
                String expectedCaseKey, int expectedSampleNumber) {
            return candidateSha256.equals(expectedCandidateSha256)
                    && outputSha256.equals(expectedOutputSha256)
                    && caseKey.equals(expectedCaseKey)
                    && sampleNumber == expectedSampleNumber;
        }
    }

    public record OracleAssessment(OracleStatus status, String reasonCode) {
        public OracleAssessment {
            Objects.requireNonNull(status, "status");
            requireText(reasonCode, "reasonCode");
        }
    }

    public enum OracleStatus { PASS, FAIL, HUMAN_REQUIRED, UNSCORABLE }

    private static OracleAssessment outcome(boolean accepted, String reason) {
        return new OracleAssessment(accepted ? OracleStatus.PASS : OracleStatus.FAIL, reason);
    }

    static String canonicalSha256(List<?> fields) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(fields.size());
                for (Object field : fields) {
                    if (field == null) {
                        output.writeInt(-1);
                    } else {
                        byte[] encoded = String.valueOf(field).getBytes(StandardCharsets.UTF_8);
                        output.writeInt(encoded.length);
                        output.write(encoded);
                    }
                }
            }
            return sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

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
// end::versioned-evaluation-corpus[]
