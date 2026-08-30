package dev.agenticintegrationpatterns.orderdesk.deployment;

import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.AccessMode;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.Capability;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.Credential;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.LifecycleBoundary;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.NetworkTarget;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.PublishRoute;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.ScaleBoundary;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.Stage;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.StateAccess;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.StoreClass;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.WorkloadBoundary;
import dev.agenticintegrationpatterns.orderdesk.deployment.DeploymentBoundaryManifest.WorkloadRole;
import dev.agenticintegrationpatterns.orderdesk.deployment.PromotionAdmission.AdmissionResult;
import dev.agenticintegrationpatterns.orderdesk.deployment.PromotionAdmission.ProtectedPromotionRecords;
import dev.agenticintegrationpatterns.orderdesk.deployment.PromotionAdmission.PromotionAuthority;
import dev.agenticintegrationpatterns.orderdesk.deployment.PromotionAdmission.ReleaseEvidence;
import dev.agenticintegrationpatterns.orderdesk.evaluation.EvaluationRunner.CandidateVersion;
import dev.agenticintegrationpatterns.orderdesk.evaluation.ReleaseGate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeploymentBoundariesTest {
    private static final String CANONICAL_CANDIDATE = "orderdesk-candidate-20.1";
    private static final String BLOCKED_DECISION = "release-decision-020502";
    private static final String IMAGE = "registry.example/orderdesk@sha256:" + "1".repeat(64);
    private static final String CONFIGURATION = "2".repeat(64);
    private static final String CORPUS_SHA256 = "3".repeat(64);
    private static final Instant NOW = Instant.parse("2026-08-28T14:00:00Z");

    @Test
    void sealedManifestBindsCandidateImageConfigurationAndEveryWorkloadBoundary() {
        CandidateVersion candidate = canonicalCandidate();
        DeploymentBoundaryManifest manifest = manifest(Stage.STAGING,
                candidate.candidateRef(), candidate.candidateSha256(), workloads());

        DeploymentBoundaryManifest verified = DeploymentBoundaryManifest.verify(
                manifest.deploymentRef(), manifest.stage(), manifest.tenantScope(),
                manifest.region(), manifest.candidateRef(), manifest.candidateSha256(),
                manifest.imageRef(), manifest.configurationSha256(), manifest.workloads(),
                manifest.manifestSha256());

        assertThat(verified.calculatedSha256()).isEqualTo(manifest.manifestSha256());
        assertThat(verified.workloads()).extracting(WorkloadBoundary::role)
                .containsExactly(WorkloadRole.ADMISSION, WorkloadRole.INVESTIGATION,
                        WorkloadRole.READ_GATEWAY, WorkloadRole.CONTROL,
                        WorkloadRole.EFFECT_EXECUTION, WorkloadRole.RECOVERY,
                        WorkloadRole.ANALYSIS);
    }

    @ParameterizedTest(name = "{0} {1} surface is rejected")
    @MethodSource("effectSurfaceCases")
    // tag::effect-free-workers-test[]
    void replayAndEvaluationCannotAcquireAnyEffectSurface(WorkloadRole role,
            EffectSurface surface, String expectedMessage) {
        WorkloadBoundary base = analysisWorker(role);

        assertThatThrownBy(() -> withEffectSurface(base, surface))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }
    // end::effect-free-workers-test[]

    @ParameterizedTest(name = "{0} publisher {1} permitted={2}")
    @MethodSource("analysisPublisherCases")
    void everyPublisherIsClassifiedForReplayAndEvaluation(WorkloadRole role,
            PublishRoute route, boolean permitted) {
        WorkloadBoundary base = analysisWorker(role);

        if (permitted) {
            assertThat(withPublishRoutes(base, Set.of(route)).publishRoutes())
                    .containsExactly(route);
        } else {
            assertThatThrownBy(() -> withPublishRoutes(base, Set.of(route)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("publisher");
        }
    }

    @ParameterizedTest(name = "{0} network {1} permitted={2}")
    @MethodSource("analysisNetworkCases")
    void everyNetworkTargetIsClassifiedForReplayAndEvaluation(WorkloadRole role,
            NetworkTarget target, boolean permitted) {
        WorkloadBoundary base = analysisWorker(role);

        if (permitted) {
            assertThat(withNetworkTargets(base, Set.of(target)).networkTargets())
                    .containsExactly(target);
        } else {
            assertThatThrownBy(() -> withNetworkTargets(base, Set.of(target)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("network path");
        }
    }

    @ParameterizedTest(name = "recovery {0} is rejected")
    @MethodSource("recoveryMutationSurfaceCases")
    void recoveryCannotAcquireAnEffectMutationSurface(EffectSurface surface,
            String expectedMessage) {
        WorkloadBoundary recovery = workloads().stream()
                .filter(workload -> workload.role() == WorkloadRole.RECOVERY)
                .findFirst().orElseThrow();

        assertThatThrownBy(() -> withEffectSurface(recovery, surface))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    void scalingAndLifecycleBoundariesRejectUnsafeValuesAtConstruction() {
        assertThatThrownBy(() -> new ScaleBoundary(3, 1, 4, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("replicas exceed partition ownership");
        assertThatThrownBy(() -> new LifecycleBoundary(60, 45, 30, true, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("terminationGraceSeconds");
        assertThatThrownBy(() -> new LifecycleBoundary(60, 30, 45, false, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("withdrawReadinessBeforeDrain");

        assertThat(new ScaleBoundary(0, 2, 8, 8).ownedKafkaPartitions()).isZero();
    }

    @Test
    void identitiesAndConsumerGroupsCannotCollideAcrossWorkloadResponsibilities() {
        CandidateVersion candidate = canonicalCandidate();
        List<WorkloadBoundary> original = workloads();
        WorkloadBoundary first = original.get(0);
        WorkloadBoundary second = original.get(3);

        assertThatThrownBy(() -> manifest(Stage.STAGING, candidate.candidateRef(),
                candidate.candidateSha256(), replace(original, 3,
                        copy(second, first.workloadIdentity(), second.consumerGroup(),
                                second.operationalOwner(), second.stateOwner(), second.stateAccess(),
                                second.scale(), second.lifecycle()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duplicate workloadIdentity");
        assertThatThrownBy(() -> manifest(Stage.STAGING, candidate.candidateRef(),
                candidate.candidateSha256(), replace(original, 3,
                        copy(second, second.workloadIdentity(), first.consumerGroup(),
                                second.operationalOwner(), second.stateOwner(), second.stateAccess(),
                                second.scale(), second.lifecycle()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duplicate consumerGroup");
    }

    @ParameterizedTest(name = "sealed manifest rejects {0} drift")
    @MethodSource("driftCases")
    void sealedManifestRejectsStateGroupIdentityScaleLifecycleAndOwnerDrift(String field,
            UnaryOperator<WorkloadBoundary> mutation) {
        CandidateVersion candidate = canonicalCandidate();
        DeploymentBoundaryManifest approved = manifest(Stage.STAGING,
                candidate.candidateRef(), candidate.candidateSha256(), workloads());
        List<WorkloadBoundary> drifted = replace(approved.workloads(), 0,
                mutation.apply(approved.workloads().get(0)));

        assertThatThrownBy(() -> DeploymentBoundaryManifest.verify(
                approved.deploymentRef(), approved.stage(), approved.tenantScope(),
                approved.region(), approved.candidateRef(), approved.candidateSha256(),
                approved.imageRef(), approved.configurationSha256(), drifted,
                approved.manifestSha256())).as(field)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("deployment manifest digest mismatch");
    }

    @Test
    // tag::blocked-candidate-promotion-test[]
    void canonicalBlockedCandidateCannotPromoteEvenWithAStageAuthority() {
        CandidateVersion candidate = canonicalCandidate();
        ReleaseEvidence blocked = blockedRelease(candidate, Stage.STAGING);
        DeploymentBoundaryManifest staging = manifest(Stage.STAGING,
                candidate.candidateRef(), candidate.candidateSha256(), workloads());
        PromotionAuthority authority = authority(blocked, staging,
                "authority:staging-020503");

        AdmissionResult result = promotion(blocked, authority).assess(
                blocked.releaseDecisionRef(), Stage.EVALUATION, staging,
                authority.authorityRef(), NOW);

        assertThat(blocked.releaseDecisionRef()).isEqualTo(BLOCKED_DECISION);
        assertThat(blocked.decision().decision()).isEqualTo(ReleaseGate.Decision.BLOCK);
        assertThat(result.decision()).isEqualTo(PromotionAdmission.Decision.BLOCK);
        assertThat(result.blockers()).containsExactly(
                "RELEASE_DECISION_BLOCKED:" + BLOCKED_DECISION);
    }
    // end::blocked-candidate-promotion-test[]

    @Test
    void unknownCallerSuppliedReleaseReferenceCannotReplaceProtectedBlockedEvidence() {
        CandidateVersion candidate = canonicalCandidate();
        ReleaseEvidence blocked = blockedRelease(candidate, Stage.STAGING);
        DeploymentBoundaryManifest staging = manifest(Stage.STAGING,
                candidate.candidateRef(), candidate.candidateSha256(), workloads());

        AdmissionResult result = promotion(blocked, null).assess(
                "release-decision-fabricated-allow", Stage.EVALUATION, staging, null, NOW);

        assertThat(result.blockers()).contains(
                "RELEASE_EVIDENCE_MISSING:release-decision-fabricated-allow",
                "STAGE_AUTHORITY_MISSING:STAGING");
    }

    @Test
    void anEligibleReleaseAloneDoesNotAuthorizeProduction() {
        CandidateVersion candidate = eligibleCandidate();
        ReleaseEvidence eligible = eligibleRelease(candidate, Stage.PRODUCTION);
        DeploymentBoundaryManifest production = manifest(Stage.PRODUCTION,
                candidate.candidateRef(), candidate.candidateSha256(), workloads());

        AdmissionResult result = promotion(eligible, null).assess(
                eligible.releaseDecisionRef(), Stage.CANARY, production, null, NOW);

        assertThat(result.decision()).isEqualTo(PromotionAdmission.Decision.BLOCK);
        assertThat(result.blockers()).containsExactly("STAGE_AUTHORITY_MISSING:PRODUCTION");
    }

    @Test
    void hypotheticalEligibleCandidateTraversesBoundedStagesWithFreshExactAuthority() {
        CandidateVersion candidate = eligibleCandidate();
        Stage current = Stage.EVALUATION;
        for (Stage target : List.of(Stage.STAGING, Stage.CANARY, Stage.PRODUCTION)) {
            ReleaseEvidence eligible = eligibleRelease(candidate, target);
            DeploymentBoundaryManifest manifest = manifest(target, candidate.candidateRef(),
                    candidate.candidateSha256(), workloads());
            PromotionAuthority authority = authority(eligible, manifest,
                    "authority:" + target.name().toLowerCase());
            AdmissionResult result = promotion(eligible, authority).assess(
                    eligible.releaseDecisionRef(), current, manifest,
                    authority.authorityRef(), NOW);

            assertThat(result.decision()).isEqualTo(PromotionAdmission.Decision.ALLOW);
            assertThat(result.blockers()).isEmpty();
            assertThat(result.qualification()).contains("neither a later-stage promotion")
                    .contains("nor business-effect authority");
            current = target;
        }
    }

    @Test
    void authorityCannotBeReusedForAnotherStageOrChangedManifest() {
        CandidateVersion candidate = eligibleCandidate();
        ReleaseEvidence stagingRelease = eligibleRelease(candidate, Stage.STAGING);
        DeploymentBoundaryManifest staging = manifest(Stage.STAGING, candidate.candidateRef(),
                candidate.candidateSha256(), workloads());
        PromotionAuthority stagingAuthority = authority(stagingRelease, staging,
                "authority:staging");
        DeploymentBoundaryManifest canary = manifest(Stage.CANARY, candidate.candidateRef(),
                candidate.candidateSha256(), workloads());

        ReleaseEvidence canaryRelease = eligibleRelease(candidate, Stage.CANARY);
        AdmissionResult wrongStage = promotion(canaryRelease, stagingAuthority).assess(
                canaryRelease.releaseDecisionRef(), Stage.STAGING, canary,
                stagingAuthority.authorityRef(), NOW);
        assertThat(wrongStage.blockers()).contains("STAGE_AUTHORITY_TARGET_MISMATCH",
                "STAGE_AUTHORITY_MANIFEST_MISMATCH");

        DeploymentBoundaryManifest changedConfig = manifest(Stage.STAGING,
                candidate.candidateRef(), candidate.candidateSha256(), workloads(), "3".repeat(64));
        AdmissionResult changed = promotion(stagingRelease, stagingAuthority).assess(
                stagingRelease.releaseDecisionRef(), Stage.EVALUATION, changedConfig,
                stagingAuthority.authorityRef(), NOW);
        assertThat(changed.blockers()).containsExactly("STAGE_AUTHORITY_MANIFEST_MISMATCH");
    }

    @Test
    void stagesCannotBeSkippedAndExpiredAuthorityCannotBeUsed() {
        CandidateVersion candidate = eligibleCandidate();
        ReleaseEvidence eligible = eligibleRelease(candidate, Stage.CANARY);
        DeploymentBoundaryManifest canary = manifest(Stage.CANARY, candidate.candidateRef(),
                candidate.candidateSha256(), workloads());
        PromotionAuthority expired = new PromotionAuthority("authority:expired",
                "workload:release-authority-service", "policy:promotion-v1",
                "role:release-owner", eligible.releaseDecisionRef(), candidate.candidateRef(),
                candidate.candidateSha256(), Stage.CANARY, canary.manifestSha256(),
                NOW.minusSeconds(120), NOW.minusSeconds(1), null, null);

        AdmissionResult result = promotion(eligible, expired).assess(
                eligible.releaseDecisionRef(), Stage.EVALUATION, canary,
                expired.authorityRef(), NOW);

        assertThat(result.blockers()).contains("NON_SEQUENTIAL_STAGE_TRANSITION:EVALUATION->CANARY",
                "STAGE_AUTHORITY_NOT_CURRENT");
    }

    @Test
    void revokedAndSupersededAuthorityIsNotCurrentPromotionAuthority() {
        CandidateVersion candidate = eligibleCandidate();
        ReleaseEvidence eligible = eligibleRelease(candidate, Stage.STAGING);
        DeploymentBoundaryManifest staging = manifest(Stage.STAGING,
                candidate.candidateRef(), candidate.candidateSha256(), workloads());
        PromotionAuthority invalid = new PromotionAuthority("authority:revoked",
                "workload:release-authority-service", "policy:promotion-v1",
                "role:release-owner", eligible.releaseDecisionRef(), candidate.candidateRef(),
                candidate.candidateSha256(), Stage.STAGING, staging.manifestSha256(),
                NOW.minusSeconds(120), NOW.plusSeconds(600), NOW.minusSeconds(1),
                "authority:replacement");

        AdmissionResult result = promotion(eligible, invalid).assess(
                eligible.releaseDecisionRef(), Stage.EVALUATION, staging,
                invalid.authorityRef(), NOW);

        assertThat(result.blockers()).containsExactly("STAGE_AUTHORITY_REVOKED",
                "STAGE_AUTHORITY_SUPERSEDED:authority:replacement");
    }

    @Test
    void preReleaseTransitionsAreOutsidePromotionAndReleaseEvidenceIsStageSpecific() {
        CandidateVersion candidate = eligibleCandidate();
        ReleaseEvidence stagingRelease = eligibleRelease(candidate, Stage.STAGING);
        DeploymentBoundaryManifest evaluation = manifest(Stage.EVALUATION,
                candidate.candidateRef(), candidate.candidateSha256(), workloads());

        AdmissionResult preRelease = promotion(stagingRelease, null).assess(
                stagingRelease.releaseDecisionRef(), Stage.DEVELOPMENT, evaluation, null, NOW);
        assertThat(preRelease.blockers()).contains(
                "PRE_RELEASE_TRANSITION_OUT_OF_SCOPE:EVALUATION",
                "NON_SEQUENTIAL_STAGE_TRANSITION:DEVELOPMENT->EVALUATION");

        DeploymentBoundaryManifest canary = manifest(Stage.CANARY, candidate.candidateRef(),
                candidate.candidateSha256(), workloads());
        PromotionAuthority canaryAuthority = authority(stagingRelease, canary,
                "authority:canary");
        AdmissionResult wrongReleaseStage = promotion(stagingRelease, canaryAuthority).assess(
                stagingRelease.releaseDecisionRef(), Stage.STAGING, canary,
                canaryAuthority.authorityRef(), NOW);
        assertThat(wrongReleaseStage.blockers()).containsExactly(
                "RELEASE_DECISION_STAGE_MISMATCH");
    }

    @Test
    void containerImageMustBePinnedByDigest() {
        CandidateVersion candidate = eligibleCandidate();

        assertThatThrownBy(() -> DeploymentBoundaryManifest.seal("orderdesk:staging",
                Stage.STAGING, "tenant-ca", "ca-central", candidate.candidateRef(),
                candidate.candidateSha256(), "registry.example/orderdesk:latest",
                CONFIGURATION, workloads())).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("imageRef");
    }

    private static Stream<Arguments> effectSurfaceCases() {
        return Stream.of(WorkloadRole.REPLAY, WorkloadRole.EVALUATION).flatMap(role -> Stream.of(
                Arguments.of(role, EffectSurface.EFFECT_CAPABILITY,
                        role + " workload has effect capability"),
                Arguments.of(role, EffectSurface.PROTECTED_CAPABILITY,
                        role + " workload has protected mutation capability"),
                Arguments.of(role, EffectSurface.PROTECTED_STATE,
                        role + " workload has protected state mutation"),
                Arguments.of(role, EffectSurface.PROTECTED_CREDENTIAL,
                        role + " workload has protected state credential"),
                Arguments.of(role, EffectSurface.MUTATION_CREDENTIAL,
                        role + " workload has effect credential"),
                Arguments.of(role, EffectSurface.OBSERVATION_CREDENTIAL,
                        role + " workload has effect credential"),
                Arguments.of(role, EffectSurface.PROTECTED_PUBLISHER,
                        role + " workload has protected mutation publisher"),
                Arguments.of(role, EffectSurface.PROTECTED_NETWORK,
                        role + " workload has protected state network path"),
                Arguments.of(role, EffectSurface.MUTATION_NETWORK,
                        role + " workload has effect network path"),
                Arguments.of(role, EffectSurface.OBSERVATION_NETWORK,
                        role + " workload has effect network path")));
    }

    private static Stream<Arguments> recoveryMutationSurfaceCases() {
        return Stream.of(
                Arguments.of(EffectSurface.EFFECT_CAPABILITY,
                        "RECOVERY workload has mutation capability"),
                Arguments.of(EffectSurface.MUTATION_CREDENTIAL,
                        "RECOVERY workload has mutation credential"),
                Arguments.of(EffectSurface.MUTATION_PUBLISHER,
                        "RECOVERY workload has mutation publisher"),
                Arguments.of(EffectSurface.MUTATION_NETWORK,
                        "RECOVERY workload has mutation network path"));
    }

    private static Stream<Arguments> analysisPublisherCases() {
        return Stream.of(WorkloadRole.REPLAY, WorkloadRole.EVALUATION).flatMap(role ->
                Stream.of(PublishRoute.values()).map(route -> Arguments.of(role, route,
                        (role == WorkloadRole.REPLAY && route == PublishRoute.REPLAY_RESULTS)
                                || (role == WorkloadRole.EVALUATION
                                && route == PublishRoute.EVALUATION_RESULTS))));
    }

    private static Stream<Arguments> analysisNetworkCases() {
        Set<NetworkTarget> permitted = Set.of(NetworkTarget.EVALUATION_BROKER,
                NetworkTarget.ARTIFACT_STORE, NetworkTarget.MODEL_PROVIDER,
                NetworkTarget.EVALUATION_STORE, NetworkTarget.TELEMETRY_COLLECTOR);
        return Stream.of(WorkloadRole.REPLAY, WorkloadRole.EVALUATION).flatMap(role ->
                Stream.of(NetworkTarget.values()).map(target ->
                        Arguments.of(role, target, permitted.contains(target))));
    }

    private static Stream<Arguments> driftCases() {
        return Stream.of(
                Arguments.of("state", mutation(base -> copy(base, base.workloadIdentity(),
                        base.consumerGroup(), base.operationalOwner(), base.stateOwner(),
                        Set.of(new StateAccess("state://changed", StoreClass.COMMAND_INBOX,
                                AccessMode.READ_WRITE)),
                        base.scale(), base.lifecycle()))),
                Arguments.of("consumer group", mutation(base -> copy(base,
                        base.workloadIdentity(), base.consumerGroup() + "-v2",
                        base.operationalOwner(), base.stateOwner(), base.stateAccess(),
                        base.scale(), base.lifecycle()))),
                Arguments.of("workload identity", mutation(base -> copy(base,
                        base.workloadIdentity() + "-v2", base.consumerGroup(),
                        base.operationalOwner(), base.stateOwner(), base.stateAccess(),
                        base.scale(), base.lifecycle()))),
                Arguments.of("scale", mutation(base -> copy(base, base.workloadIdentity(),
                        base.consumerGroup(), base.operationalOwner(), base.stateOwner(),
                        base.stateAccess(), new ScaleBoundary(base.scale().ownedKafkaPartitions(),
                                base.scale().minimumReplicas(), base.scale().maximumReplicas(),
                                base.scale().maxInFlightPerReplica() + 1), base.lifecycle()))),
                Arguments.of("lifecycle", mutation(base -> copy(base,
                        base.workloadIdentity(), base.consumerGroup(), base.operationalOwner(),
                        base.stateOwner(), base.stateAccess(), base.scale(),
                        new LifecycleBoundary(base.lifecycle().startupTimeoutSeconds() + 1,
                                base.lifecycle().drainTimeoutSeconds(),
                                base.lifecycle().terminationGraceSeconds(), true, true)))),
                Arguments.of("operational owner", mutation(base -> copy(base,
                        base.workloadIdentity(), base.consumerGroup(),
                        base.operationalOwner() + "-v2", base.stateOwner(), base.stateAccess(),
                        base.scale(), base.lifecycle()))),
                Arguments.of("state owner", mutation(base -> copy(base,
                        base.workloadIdentity(), base.consumerGroup(), base.operationalOwner(),
                        base.stateOwner() + "-v2", base.stateAccess(), base.scale(),
                        base.lifecycle()))));
    }

    private static UnaryOperator<WorkloadBoundary> mutation(
            UnaryOperator<WorkloadBoundary> mutation) {
        return mutation;
    }

    private static WorkloadBoundary withEffectSurface(WorkloadBoundary base,
            EffectSurface surface) {
        Set<Capability> capabilities = base.capabilities();
        Set<Credential> credentials = base.credentials();
        Set<PublishRoute> publishers = base.publishRoutes();
        Set<NetworkTarget> networks = base.networkTargets();
        Set<StateAccess> stateAccess = base.stateAccess();
        switch (surface) {
            case EFFECT_CAPABILITY -> capabilities = plus(capabilities, Capability.EXECUTE_EFFECT);
            case PROTECTED_CAPABILITY -> capabilities = plus(capabilities,
                    Capability.HANDLE_APPROVAL);
            case PROTECTED_STATE -> stateAccess = plus(stateAccess,
                    new StateAccess("sql://effect-ledger", StoreClass.EFFECT_LEDGER,
                            AccessMode.READ_WRITE));
            case PROTECTED_CREDENTIAL -> credentials = plus(credentials,
                    Credential.APPROVAL_STORE);
            case MUTATION_CREDENTIAL -> credentials = plus(credentials, Credential.EFFECT_TARGET);
            case OBSERVATION_CREDENTIAL -> credentials = plus(credentials,
                    Credential.EFFECT_OBSERVER);
            case PROTECTED_PUBLISHER -> publishers = plus(publishers,
                    PublishRoute.APPROVAL_EVENTS);
            case MUTATION_PUBLISHER -> publishers = plus(publishers,
                    PublishRoute.EFFECT_COMMANDS);
            case PROTECTED_NETWORK -> networks = plus(networks, NetworkTarget.PROCESS_STORE);
            case MUTATION_NETWORK -> networks = plus(networks, NetworkTarget.EFFECT_TARGET);
            case OBSERVATION_NETWORK -> networks = plus(networks,
                    NetworkTarget.EFFECT_OBSERVATION);
        }
        return new WorkloadBoundary(base.workloadId(), base.role(), base.operationalOwner(),
                base.workloadIdentity(), base.stateOwner(), base.consumerGroup(),
                stateAccess, base.scale(), base.lifecycle(), capabilities, credentials,
                publishers, networks);
    }

    private static WorkloadBoundary withPublishRoutes(WorkloadBoundary base,
            Set<PublishRoute> publishRoutes) {
        return new WorkloadBoundary(base.workloadId(), base.role(), base.operationalOwner(),
                base.workloadIdentity(), base.stateOwner(), base.consumerGroup(),
                base.stateAccess(), base.scale(), base.lifecycle(), base.capabilities(),
                base.credentials(), publishRoutes, base.networkTargets());
    }

    private static WorkloadBoundary withNetworkTargets(WorkloadBoundary base,
            Set<NetworkTarget> networkTargets) {
        return new WorkloadBoundary(base.workloadId(), base.role(), base.operationalOwner(),
                base.workloadIdentity(), base.stateOwner(), base.consumerGroup(),
                base.stateAccess(), base.scale(), base.lifecycle(), base.capabilities(),
                base.credentials(), base.publishRoutes(), networkTargets);
    }

    private static <T> Set<T> plus(Set<T> original, T value) {
        java.util.HashSet<T> copy = new java.util.HashSet<>(original);
        copy.add(value);
        return Set.copyOf(copy);
    }

    private static CandidateVersion canonicalCandidate() {
        return CandidateVersion.seal(CANONICAL_CANDIDATE, "model:fixture-v2",
                "provider:fixture-b", "instruction:desk-v8", "tools:catalog-v4",
                "config:desk-v13");
    }

    private static CandidateVersion eligibleCandidate() {
        return CandidateVersion.seal("orderdesk-candidate-21.1-eligible-fixture",
                "model:fixture-v3", "provider:fixture-b", "instruction:desk-v9",
                "tools:catalog-v4", "config:desk-v14");
    }

    private static ReleaseEvidence blockedRelease(CandidateVersion candidate, Stage targetStage) {
        return new ReleaseEvidence(BLOCKED_DECISION, "eval-run-020501", targetStage,
                releaseDecision(candidate, ReleaseGate.Decision.BLOCK,
                        List.of("BLOCKING_CANDIDATE_FINDING")));
    }

    private static ReleaseEvidence eligibleRelease(CandidateVersion candidate,
            Stage targetStage) {
        String suffix = targetStage.name().toLowerCase();
        return new ReleaseEvidence("release-decision-021-fixture-" + suffix,
                "eval-run-021-fixture-" + suffix, targetStage,
                releaseDecision(candidate, ReleaseGate.Decision.ALLOW, List.of()));
    }

    private static ReleaseGate.ReleaseDecision releaseDecision(CandidateVersion candidate,
            ReleaseGate.Decision decision, List<String> blockers) {
        return new ReleaseGate.ReleaseDecision("policy:release-20-v1",
                "orderdesk-eval-corpus-20-v1", "v1", CORPUS_SHA256,
                candidate.candidateRef(), candidate.candidateSha256(), decision,
                "role:model-risk-owner", blockers, List.of());
    }

    private static PromotionAuthority authority(ReleaseEvidence release,
            DeploymentBoundaryManifest manifest, String authorityRef) {
        return new PromotionAuthority(authorityRef, "workload:release-authority-service",
                "policy:promotion-v1", "role:release-owner",
                release.releaseDecisionRef(), release.candidateRef(), release.candidateSha256(),
                manifest.stage(), manifest.manifestSha256(), NOW.minusSeconds(60),
                NOW.plusSeconds(600), null, null);
    }

    private static PromotionAdmission promotion(ReleaseEvidence release,
            PromotionAuthority authority) {
        return new PromotionAdmission(new ProtectedPromotionRecords() {
            @Override
            public Optional<ReleaseEvidence> releaseEvidence(String releaseDecisionRef) {
                return release != null && release.releaseDecisionRef().equals(releaseDecisionRef)
                        ? Optional.of(release) : Optional.empty();
            }

            @Override
            public Optional<PromotionAuthority> promotionAuthority(String authorityRef) {
                return authority != null && authority.authorityRef().equals(authorityRef)
                        ? Optional.of(authority) : Optional.empty();
            }
        });
    }

    private static DeploymentBoundaryManifest manifest(Stage stage, String candidateRef,
            String candidateSha256, List<WorkloadBoundary> workloads) {
        return manifest(stage, candidateRef, candidateSha256, workloads, CONFIGURATION);
    }

    private static DeploymentBoundaryManifest manifest(Stage stage, String candidateRef,
            String candidateSha256, List<WorkloadBoundary> workloads, String configurationSha256) {
        return DeploymentBoundaryManifest.seal("orderdesk:" + stage.name().toLowerCase(), stage,
                "tenant-ca", "ca-central", candidateRef, candidateSha256, IMAGE,
                configurationSha256, workloads);
    }

    private static List<WorkloadBoundary> workloads() {
        LifecycleBoundary lifecycle = new LifecycleBoundary(60, 30, 45, true, true);
        return List.of(
                new WorkloadBoundary("admission", WorkloadRole.ADMISSION, "team:orderdesk",
                        "spiffe://example/orderdesk/admission", "team:orderdesk-data",
                        "orderdesk-admission-v1", Set.of(new StateAccess("sql://command-inbox",
                                StoreClass.COMMAND_INBOX, AccessMode.READ_WRITE)),
                        new ScaleBoundary(6, 2, 4, 32), lifecycle,
                        Set.of(Capability.ADMIT_COMMAND),
                        Set.of(Credential.BROKER, Credential.PROCESS_STORE, Credential.TELEMETRY),
                        Set.of(PublishRoute.INVESTIGATION_WORK, PublishRoute.LIFECYCLE_EVENTS),
                        Set.of(NetworkTarget.OPERATIONAL_BROKER, NetworkTarget.PROCESS_STORE,
                                NetworkTarget.TELEMETRY_COLLECTOR)),
                new WorkloadBoundary("investigation", WorkloadRole.INVESTIGATION,
                        "team:orderdesk-ai", "spiffe://example/orderdesk/investigation",
                        "team:orderdesk-data", null,
                        Set.of(new StateAccess("sql://admitted-work", StoreClass.PROCESS_STATE,
                                AccessMode.READ_WRITE)),
                        new ScaleBoundary(0, 2, 8, 8), lifecycle,
                        Set.of(Capability.INVESTIGATE),
                        Set.of(Credential.BROKER, Credential.PROCESS_STORE,
                                Credential.MODEL_PROVIDER, Credential.TELEMETRY),
                        Set.of(PublishRoute.LIFECYCLE_EVENTS),
                        Set.of(NetworkTarget.OPERATIONAL_BROKER, NetworkTarget.PROCESS_STORE,
                                NetworkTarget.MODEL_PROVIDER, NetworkTarget.TELEMETRY_COLLECTOR)),
                new WorkloadBoundary("read-gateway", WorkloadRole.READ_GATEWAY,
                        "team:orderdesk-data", "spiffe://example/orderdesk/read-gateway",
                        "team:orderdesk-data", null,
                        Set.of(new StateAccess("object://artifacts", StoreClass.ARTIFACTS,
                                AccessMode.READ_WRITE)),
                        new ScaleBoundary(0, 2, 6, 8), lifecycle,
                        Set.of(Capability.RESOLVE_CONTEXT),
                        Set.of(Credential.ARTIFACT_STORE, Credential.SOURCE_SYSTEM,
                                Credential.TELEMETRY), Set.of(),
                        Set.of(NetworkTarget.ARTIFACT_STORE, NetworkTarget.READ_SOURCE,
                                NetworkTarget.TELEMETRY_COLLECTOR)),
                new WorkloadBoundary("control", WorkloadRole.CONTROL, "team:orderdesk",
                        "spiffe://example/orderdesk/control", "team:orderdesk-data",
                        "orderdesk-control-v1", Set.of(
                                new StateAccess("sql://process-state", StoreClass.PROCESS_STATE,
                                        AccessMode.READ_WRITE),
                                new StateAccess("sql://approval-state", StoreClass.APPROVAL_STATE,
                                        AccessMode.READ_WRITE)),
                        new ScaleBoundary(6, 2, 4, 32), lifecycle,
                        Set.of(Capability.MANAGE_PROCESS, Capability.HANDLE_APPROVAL),
                        Set.of(Credential.BROKER, Credential.PROCESS_STORE,
                                Credential.APPROVAL_STORE, Credential.TELEMETRY),
                        Set.of(PublishRoute.LIFECYCLE_EVENTS, PublishRoute.APPROVAL_EVENTS,
                                PublishRoute.EFFECT_COMMANDS),
                        Set.of(NetworkTarget.OPERATIONAL_BROKER, NetworkTarget.PROCESS_STORE,
                                NetworkTarget.APPROVAL_STORE, NetworkTarget.TELEMETRY_COLLECTOR)),
                new WorkloadBoundary("effect", WorkloadRole.EFFECT_EXECUTION,
                        "team:order-operations", "spiffe://example/orderdesk/effect",
                        "team:order-operations", "orderdesk-effect-v1",
                        Set.of(new StateAccess("sql://effect-ledger", StoreClass.EFFECT_LEDGER,
                                AccessMode.READ_WRITE)),
                        new ScaleBoundary(6, 2, 4, 4), lifecycle,
                        Set.of(Capability.EXECUTE_EFFECT),
                        Set.of(Credential.BROKER, Credential.PROCESS_STORE,
                                Credential.EFFECT_TARGET, Credential.TELEMETRY),
                        Set.of(PublishRoute.EFFECT_OUTCOMES),
                        Set.of(NetworkTarget.OPERATIONAL_BROKER, NetworkTarget.PROCESS_STORE,
                                NetworkTarget.EFFECT_TARGET, NetworkTarget.TELEMETRY_COLLECTOR)),
                new WorkloadBoundary("recovery", WorkloadRole.RECOVERY,
                        "team:order-operations", "spiffe://example/orderdesk/recovery",
                        "team:order-operations", null,
                        Set.of(new StateAccess("sql://effect-ledger", StoreClass.EFFECT_LEDGER,
                                AccessMode.READ_WRITE)),
                        new ScaleBoundary(0, 1, 2, 2), lifecycle,
                        Set.of(Capability.RECONCILE_EFFECT),
                        Set.of(Credential.BROKER, Credential.PROCESS_STORE,
                                Credential.EFFECT_OBSERVER, Credential.TELEMETRY),
                        Set.of(PublishRoute.EFFECT_OUTCOMES),
                        Set.of(NetworkTarget.OPERATIONAL_BROKER, NetworkTarget.PROCESS_STORE,
                                NetworkTarget.EFFECT_OBSERVATION,
                                NetworkTarget.TELEMETRY_COLLECTOR)),
                combinedAnalysisWorker());
    }

    private static WorkloadBoundary combinedAnalysisWorker() {
        return new WorkloadBoundary("analysis", WorkloadRole.ANALYSIS,
                "team:model-assurance", "spiffe://example/orderdesk/analysis",
                "team:model-assurance-data", null, Set.of(
                        new StateAccess("object://retained-history", StoreClass.RETAINED_HISTORY,
                                AccessMode.READ_ONLY),
                        new StateAccess("object://eval-corpus", StoreClass.EVALUATION_CORPUS,
                                AccessMode.READ_ONLY),
                        new StateAccess("sql://analysis-results", StoreClass.ANALYSIS_RESULTS,
                                AccessMode.READ_WRITE)),
                new ScaleBoundary(0, 1, 4, 2),
                new LifecycleBoundary(60, 30, 45, true, true),
                Set.of(Capability.READ_HISTORY, Capability.REPLAY_HISTORY,
                        Capability.EVALUATE_CANDIDATE),
                Set.of(Credential.ARTIFACT_STORE, Credential.EVALUATION_STORE,
                        Credential.MODEL_PROVIDER, Credential.TELEMETRY),
                Set.of(PublishRoute.REPLAY_RESULTS, PublishRoute.EVALUATION_RESULTS),
                Set.of(NetworkTarget.EVALUATION_BROKER, NetworkTarget.ARTIFACT_STORE,
                        NetworkTarget.EVALUATION_STORE, NetworkTarget.MODEL_PROVIDER,
                        NetworkTarget.TELEMETRY_COLLECTOR));
    }

    private static WorkloadBoundary analysisWorker(WorkloadRole role) {
        String suffix = role.name().toLowerCase();
        boolean replay = role == WorkloadRole.REPLAY;
        return new WorkloadBoundary(suffix, role, "team:model-assurance",
                "spiffe://example/orderdesk/" + suffix, "team:model-assurance-data",
                null, Set.of(
                        new StateAccess(replay ? "object://retained-history" : "object://eval-corpus",
                                replay ? StoreClass.RETAINED_HISTORY : StoreClass.EVALUATION_CORPUS,
                                AccessMode.READ_ONLY),
                        new StateAccess("sql://" + suffix + "-results",
                                StoreClass.ANALYSIS_RESULTS, AccessMode.READ_WRITE)),
                new ScaleBoundary(0, 1, 2, 2),
                new LifecycleBoundary(60, 30, 45, true, true),
                replay ? Set.of(Capability.READ_HISTORY, Capability.REPLAY_HISTORY)
                        : Set.of(Capability.EVALUATE_CANDIDATE),
                Set.of(Credential.ARTIFACT_STORE, Credential.EVALUATION_STORE,
                        Credential.MODEL_PROVIDER, Credential.TELEMETRY),
                Set.of(replay ? PublishRoute.REPLAY_RESULTS : PublishRoute.EVALUATION_RESULTS),
                Set.of(NetworkTarget.EVALUATION_BROKER, NetworkTarget.ARTIFACT_STORE,
                        NetworkTarget.EVALUATION_STORE, NetworkTarget.MODEL_PROVIDER,
                        NetworkTarget.TELEMETRY_COLLECTOR));
    }

    private static WorkloadBoundary copy(WorkloadBoundary base, String identity,
            String consumerGroup, String operationalOwner, String stateOwner,
            Set<StateAccess> stateAccess, ScaleBoundary scale, LifecycleBoundary lifecycle) {
        return new WorkloadBoundary(base.workloadId(), base.role(), operationalOwner, identity,
                stateOwner, consumerGroup, stateAccess, scale, lifecycle, base.capabilities(),
                base.credentials(), base.publishRoutes(), base.networkTargets());
    }

    private static List<WorkloadBoundary> replace(List<WorkloadBoundary> original, int index,
            WorkloadBoundary replacement) {
        List<WorkloadBoundary> copy = new ArrayList<>(original);
        copy.set(index, replacement);
        return List.copyOf(copy);
    }

    private enum EffectSurface {
        EFFECT_CAPABILITY, PROTECTED_CAPABILITY, PROTECTED_STATE, PROTECTED_CREDENTIAL,
        MUTATION_CREDENTIAL, OBSERVATION_CREDENTIAL, PROTECTED_PUBLISHER,
        MUTATION_PUBLISHER, PROTECTED_NETWORK, MUTATION_NETWORK, OBSERVATION_NETWORK
    }
}
