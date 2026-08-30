package dev.agenticintegrationpatterns.orderdesk.context;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

@Component
public class JdbcContextSnapshotStore {
    private final JdbcTemplate jdbc;

    public JdbcContextSnapshotStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<ContextSnapshot> findByRun(String tenantId, String runId) {
        var rows = jdbc.query("""
                select snapshot_id, command_id, admitted_work_fingerprint,
                       instruction_set_ref, policy_set_ref, capability_catalog_ref,
                       created_at, selection_policy_version,
                       token_estimator_version, max_context_bytes, max_estimated_tokens,
                       used_context_bytes, estimated_tokens
                  from context_snapshot
                 where tenant_id=? and run_id=?
                """, (rs, row) -> new ContextSnapshot(
                        1,
                        rs.getString("snapshot_id"),
                        tenantId,
                        runId,
                        rs.getString("command_id"),
                        rs.getString("admitted_work_fingerprint"),
                        rs.getString("instruction_set_ref"),
                        rs.getString("policy_set_ref"),
                        rs.getString("capability_catalog_ref"),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getString("selection_policy_version"),
                        rs.getString("token_estimator_version"),
                        rs.getInt("max_context_bytes"),
                        rs.getInt("max_estimated_tokens"),
                        rs.getInt("used_context_bytes"),
                        rs.getInt("estimated_tokens"),
                        loadArtifacts(tenantId, rs.getString("snapshot_id"))),
                tenantId, runId);
        return rows.stream().findFirst();
    }

    private List<ContextArtifact> loadArtifacts(String tenantId, String snapshotId) {
        return jdbc.query("""
                select a.artifact_id, v.view_id, a.source_reference, a.source_system,
                       a.source_version,
                       i.observed_at as item_observed_at,
                       i.retrieved_at as item_retrieved_at,
                       i.valid_until as item_valid_until,
                       i.content_type as item_content_type,
                       i.trust_class as item_trust_class,
                       a.source_size_bytes, a.source_sha256,
                       a.source_bytes,
                       v.view_size_bytes, v.view_sha256, v.normalization_version,
                       v.redaction_policy_version, v.model_safe_text
                  from context_snapshot_item i
                  join artifact_content a on a.tenant_id=i.tenant_id
                                         and a.artifact_id=i.artifact_id
                  join artifact_view v on v.tenant_id=i.tenant_id
                                      and v.view_id=i.view_id
                 where i.tenant_id=? and i.snapshot_id=?
                 order by i.item_ordinal
                """, (rs, row) -> {
                    byte[] sourceBytes = rs.getBytes("source_bytes");
                    String modelSafeText = rs.getString("model_safe_text");
                    if (!rs.getString("source_sha256").equals(ArtifactDigest.sha256(sourceBytes))
                            || !rs.getString("view_sha256").equals(ArtifactDigest.sha256(
                            modelSafeText.getBytes(java.nio.charset.StandardCharsets.UTF_8)))) {
                        throw new ContextResolutionException(
                                ContextResolutionException.Reason.INTEGRITY_MISMATCH,
                                rs.getString("source_reference"),
                                "Stored artifact bytes do not match the manifest digest");
                    }
                    return new ContextArtifact(
                        rs.getString("artifact_id"),
                        rs.getString("view_id"),
                        rs.getString("source_reference"),
                        rs.getString("source_system"),
                        rs.getString("source_version"),
                        rs.getTimestamp("item_observed_at").toInstant(),
                        rs.getTimestamp("item_retrieved_at").toInstant(),
                        rs.getTimestamp("item_valid_until").toInstant(),
                        rs.getString("item_content_type"),
                        InvestigateOrderException.EvidenceTrust.valueOf(
                                rs.getString("item_trust_class")),
                        rs.getInt("source_size_bytes"),
                        rs.getString("source_sha256"),
                        rs.getInt("view_size_bytes"),
                        rs.getString("view_sha256"),
                        rs.getString("normalization_version"),
                        rs.getString("redaction_policy_version"),
                        modelSafeText);
                },
                tenantId, snapshotId);
    }

    // tag::snapshot-transaction[]
    @Transactional
    public ContextSnapshot save(ContextSnapshot snapshot, List<PreparedContextArtifact> prepared) {
        var existing = findByRun(snapshot.tenantId(), snapshot.runId());
        if (existing.isPresent()) {
            if (!existing.get().admittedWorkFingerprint()
                    .equals(snapshot.admittedWorkFingerprint())) {
                throw new ContextResolutionException(
                        ContextResolutionException.Reason.RUN_SNAPSHOT_COLLISION,
                        snapshot.runId(),
                        "Run ID is already bound to different admitted work");
            }
            return existing.get();
        }

        for (var item : prepared) {
            saveArtifactIfAbsent(snapshot.tenantId(), item);
        }
        jdbc.update("""
                insert into context_snapshot
                (tenant_id, snapshot_id, run_id, command_id, admitted_work_fingerprint,
                 instruction_set_ref, policy_set_ref, capability_catalog_ref,
                 created_at, selection_policy_version,
                 token_estimator_version, max_context_bytes, max_estimated_tokens,
                 used_context_bytes, estimated_tokens)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, snapshot.tenantId(), snapshot.snapshotId(), snapshot.runId(),
                snapshot.commandId(), snapshot.admittedWorkFingerprint(),
                snapshot.instructionSetRef(), snapshot.policySetRef(),
                snapshot.capabilityCatalogRef(), Timestamp.from(snapshot.createdAt()),
                snapshot.selectionPolicyVersion(),
                snapshot.tokenEstimatorVersion(), snapshot.maxContextBytes(),
                snapshot.maxEstimatedTokens(), snapshot.usedContextBytes(),
                snapshot.estimatedTokens());

        for (int index = 0; index < snapshot.artifacts().size(); index++) {
            var artifact = snapshot.artifacts().get(index);
            jdbc.update("""
                    insert into context_snapshot_item
                    (tenant_id, snapshot_id, item_ordinal, artifact_id, view_id,
                     observed_at, retrieved_at, valid_until, content_type, trust_class)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, snapshot.tenantId(), snapshot.snapshotId(), index,
                    artifact.artifactId(), artifact.viewId(),
                    Timestamp.from(artifact.observedAt()),
                    Timestamp.from(artifact.retrievedAt()),
                    Timestamp.from(artifact.validUntil()), artifact.contentType(),
                    artifact.trust().name());
        }
        return snapshot;
    }
    // end::snapshot-transaction[]

    private void saveArtifactIfAbsent(String tenantId, PreparedContextArtifact prepared) {
        var artifact = prepared.artifact();
        var sameSourceVersion = jdbc.query("""
                select source_sha256 from artifact_content
                 where tenant_id=? and source_system=?
                   and source_reference=? and source_version=?
                """, (rs, row) -> rs.getString(1), tenantId, artifact.sourceSystem(),
                artifact.reference(), artifact.sourceVersion());
        if (!sameSourceVersion.isEmpty()
                && !sameSourceVersion.get(0).equals(artifact.sourceSha256())) {
            throw new ContextResolutionException(
                    ContextResolutionException.Reason.INTEGRITY_MISMATCH,
                    artifact.reference(),
                    "A source version was reused for different content bytes");
        }
        Integer artifactCount = jdbc.queryForObject("""
                select count(*) from artifact_content where tenant_id=? and artifact_id=?
                """, Integer.class, tenantId, artifact.artifactId());
        if (artifactCount != null && artifactCount == 0) {
            jdbc.update("""
                    insert into artifact_content
                    (tenant_id, artifact_id, source_reference, source_system, source_version,
                     observed_at, retrieved_at, valid_until, content_type, trust_class,
                     source_size_bytes, source_sha256, source_bytes)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, tenantId, artifact.artifactId(), artifact.reference(),
                    artifact.sourceSystem(), artifact.sourceVersion(),
                    Timestamp.from(artifact.observedAt()), Timestamp.from(artifact.retrievedAt()),
                    Timestamp.from(artifact.validUntil()), artifact.contentType(),
                    artifact.trust().name(), artifact.sourceSizeBytes(),
                    artifact.sourceSha256(), prepared.sourceBytes());
        }

        Integer viewCount = jdbc.queryForObject("""
                select count(*) from artifact_view where tenant_id=? and view_id=?
                """, Integer.class, tenantId, artifact.viewId());
        if (viewCount != null && viewCount == 0) {
            jdbc.update("""
                    insert into artifact_view
                    (tenant_id, view_id, artifact_id, normalization_version,
                     redaction_policy_version, view_size_bytes, view_sha256, model_safe_text)
                    values (?, ?, ?, ?, ?, ?, ?, ?)
                    """, tenantId, artifact.viewId(), artifact.artifactId(),
                    artifact.normalizationVersion(), artifact.redactionPolicyVersion(),
                    artifact.viewSizeBytes(), artifact.viewSha256(), artifact.modelSafeText());
        }
    }
}
