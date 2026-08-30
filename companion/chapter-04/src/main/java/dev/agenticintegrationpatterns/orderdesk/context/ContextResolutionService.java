package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static dev.agenticintegrationpatterns.orderdesk.context.ContextResolutionException.Reason.*;

@Component
public final class ContextResolutionService {
    private final List<ArtifactSource> sources;
    private final ArtifactNormalizer normalizer;
    private final EvidenceRedactor redactor;
    private final TokenEstimator tokenEstimator;
    private final ContextPolicy policy;
    private final JdbcContextSnapshotStore store;
    private final ObjectMapper mapper;
    private final Clock clock;

    public ContextResolutionService(
            List<ArtifactSource> sources,
            ArtifactNormalizer normalizer,
            EvidenceRedactor redactor,
            TokenEstimator tokenEstimator,
            ContextPolicy policy,
            JdbcContextSnapshotStore store,
            ObjectMapper mapper,
            Clock clock) {
        this.sources = List.copyOf(sources);
        this.normalizer = normalizer;
        this.redactor = redactor;
        this.tokenEstimator = tokenEstimator;
        this.policy = policy;
        this.store = store;
        this.mapper = mapper;
        this.clock = clock;
    }

    // tag::resolve-and-snapshot[]
    public ResolvedInvestigationContext resolve(ContextResolutionRequest request) {
        validateRequest(request);
        var admitted = request.admitted();
        var command = admitted.command();
        String tenantId = admitted.trustedContext().tenantId();
        String admittedWorkFingerprint = AdmittedWorkFingerprint.compute(mapper, admitted);

        var existing = store.findByRun(tenantId, request.runId());
        if (existing.isPresent()) {
            if (!existing.get().admittedWorkFingerprint().equals(admittedWorkFingerprint)) {
                throw new ContextResolutionException(
                        RUN_SNAPSHOT_COLLISION, request.runId(),
                        "Run ID is already bound to different admitted work");
            }
            return result(admitted, existing.get());
        }

        var retrievedAt = clock.instant();
        var prepared = new ArrayList<PreparedContextArtifact>();
        int usedBytes = 0;
        int estimatedTokens = 0;

        for (var reference : command.evidence()) {
            var matchingSources = sources.stream()
                    .filter(candidate -> candidate.supports(reference.sourceSystem()))
                    .toList();
            if (matchingSources.size() != 1) {
                throw failure(SOURCE_NOT_ALLOWED, reference,
                        "Exactly one governed source adapter must be registered");
            }
            var source = matchingSources.get(0);
            var acquired = source.acquire(tenantId, reference);
            validateAcquired(tenantId, reference, acquired, retrievedAt);

            byte[] sourceBytes = acquired.content();
            if (sourceBytes.length > policy.maxArtifactBytes()) {
                throw failure(ARTIFACT_TOO_LARGE, reference,
                        "Artifact exceeds the decoded-size limit");
            }
            String normalized = normalizer.normalize(acquired);
            String safeText = redactor.redact(normalized);
            byte[] safeBytes = safeText.getBytes(StandardCharsets.UTF_8);
            int artifactTokens = tokenEstimator.estimate(safeText);
            usedBytes += safeBytes.length;
            estimatedTokens += artifactTokens;
            if (usedBytes > policy.maxContextBytes()
                    || estimatedTokens > policy.maxEstimatedTokens()) {
                throw failure(CONTEXT_BUDGET_EXCEEDED, reference,
                        "Required evidence exceeds the context budget");
            }

            String sourceHash = ArtifactDigest.sha256(sourceBytes);
            String viewHash = ArtifactDigest.sha256(safeBytes);
            String artifactId = stableId("artifact", tenantId, acquired.reference(),
                    acquired.sourceVersion(), sourceHash);
            String viewId = stableId("view", tenantId, artifactId,
                    policy.normalizationVersion(), policy.redactionPolicyVersion(), viewHash);
            var artifact = new ContextArtifact(
                    artifactId, viewId, acquired.reference(), acquired.sourceSystem(),
                    acquired.sourceVersion(), acquired.observedAt(), retrievedAt,
                    acquired.validUntil(), acquired.contentType(), acquired.trust(),
                    sourceBytes.length, sourceHash, safeBytes.length, viewHash,
                    policy.normalizationVersion(), policy.redactionPolicyVersion(), safeText);
            prepared.add(new PreparedContextArtifact(artifact, sourceBytes));
        }

        String snapshotId = stableId("snapshot", tenantId, request.runId(),
                policy.selectionPolicyVersion());
        var snapshot = new ContextSnapshot(
                1, snapshotId, tenantId, request.runId(), command.commandId(),
                admittedWorkFingerprint,
                command.configuration().instructionSetRef(),
                command.configuration().policySetRef(),
                command.configuration().capabilityCatalogRef(),
                retrievedAt,
                policy.selectionPolicyVersion(), tokenEstimator.version(),
                policy.maxContextBytes(), policy.maxEstimatedTokens(),
                usedBytes, estimatedTokens,
                prepared.stream().map(PreparedContextArtifact::artifact).toList());
        return result(admitted, store.save(snapshot, prepared));
    }
    // end::resolve-and-snapshot[]

    private static void validateRequest(ContextResolutionRequest request) {
        if (request == null || request.runId() == null || request.runId().isBlank()
                || request.admitted() == null || request.admitted().command() == null
                || request.admitted().trustedContext() == null
                || request.admitted().command().tenantId() == null
                || request.admitted().trustedContext().tenantId() == null
                || request.admitted().command().evidence() == null
                || request.admitted().command().evidence().isEmpty()) {
            throw new ContextResolutionException(
                    INVALID_REQUEST, null, "Admitted run and evidence references are required");
        }
        if (!request.admitted().command().tenantId()
                .equals(request.admitted().trustedContext().tenantId())) {
            throw new ContextResolutionException(
                    TENANT_MISMATCH, null, "Admitted tenant identity is inconsistent");
        }
    }

    private static void validateAcquired(
            String tenantId,
            InvestigateOrderException.EvidenceReference expected,
            SourceArtifact actual,
            java.time.Instant retrievedAt) {
        if (actual == null) {
            throw failure(ARTIFACT_MISSING, expected, "Referenced evidence was not found");
        }
        if (!tenantId.equals(actual.tenantId())) {
            throw failure(TENANT_MISMATCH, expected, "Artifact belongs to another tenant");
        }
        if (!expected.reference().equals(actual.reference())
                || !expected.sourceSystem().equals(actual.sourceSystem())
                || !expected.trust().equals(actual.trust())) {
            throw failure(METADATA_MISMATCH, expected,
                    "Resolved artifact metadata does not match the admitted reference");
        }
        if (!expected.sourceVersion().equals(actual.sourceVersion())) {
            throw failure(VERSION_CHANGED, expected,
                    "Source version changed after the command was admitted");
        }
        if (actual.observedAt() == null || actual.validUntil() == null
                || !expected.observedAt().equals(actual.observedAt())
                || !expected.validUntil().equals(actual.validUntil())) {
            throw failure(METADATA_MISMATCH, expected,
                    "Resolved artifact time metadata does not match the admitted reference");
        }
        if (!actual.validUntil().isAfter(retrievedAt)) {
            throw failure(STALE_EVIDENCE, expected,
                    "Evidence expired before the context snapshot was created");
        }
        if (actual.content() == null || actual.content().length == 0) {
            throw failure(ARTIFACT_MISSING, expected, "Referenced evidence is empty");
        }
    }

    private static ContextResolutionException failure(
            ContextResolutionException.Reason reason,
            InvestigateOrderException.EvidenceReference reference,
            String message) {
        return new ContextResolutionException(reason,
                reference == null ? null : reference.reference(), message);
    }

    private static String stableId(String type, String... parts) {
        var scoped = new StringBuilder(type.length() + 32).append(type);
        for (String part : parts) {
            scoped.append('|').append(part.length()).append(':').append(part);
        }
        return type + "-" + UUID.nameUUIDFromBytes(
                scoped.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static ResolvedInvestigationContext result(
            dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation admitted,
            ContextSnapshot snapshot) {
        List<ModelContextProjection.EvidenceBlock> blocks = snapshot.artifacts().stream()
                .map(artifact -> new ModelContextProjection.EvidenceBlock(
                        artifact.artifactId(), artifact.reference(), artifact.trust(),
                        artifact.modelSafeText()))
                .toList();
        return new ResolvedInvestigationContext(admitted, snapshot,
                new ModelContextProjection(snapshot.snapshotId(),
                        snapshot.instructionSetRef(), blocks));
    }
}
