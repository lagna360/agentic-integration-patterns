package dev.agenticintegrationpatterns.orderdesk.deployment;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A book-local proof that an approved deployment boundary can be sealed and checked for drift.
 * It is a responsibility manifest, not a scheduler, service discovery system, or orchestrator.
 */
// tag::sealed-deployment-boundaries[]
public final class DeploymentBoundaryManifest {
    private final String deploymentRef;
    private final Stage stage;
    private final String tenantScope;
    private final String region;
    private final String candidateRef;
    private final String candidateSha256;
    private final String imageRef;
    private final String configurationSha256;
    private final List<WorkloadBoundary> workloads;
    private final String manifestSha256;

    private DeploymentBoundaryManifest(String deploymentRef, Stage stage, String tenantScope,
            String region, String candidateRef, String candidateSha256, String imageRef,
            String configurationSha256, List<WorkloadBoundary> workloads,
            String claimedManifestSha256) {
        this.deploymentRef = requireText(deploymentRef, "deploymentRef");
        this.stage = Objects.requireNonNull(stage, "stage");
        if (stage == Stage.UNDEPLOYED) throw new IllegalArgumentException("stage");
        this.tenantScope = requireText(tenantScope, "tenantScope");
        this.region = requireText(region, "region");
        this.candidateRef = requireText(candidateRef, "candidateRef");
        this.candidateSha256 = requireSha(candidateSha256, "candidateSha256");
        this.imageRef = requireImageDigest(imageRef);
        this.configurationSha256 = requireSha(configurationSha256,
                "configurationSha256");
        this.workloads = List.copyOf(workloads);
        if (this.workloads.isEmpty()) throw new IllegalArgumentException("workloads");
        requireUnique(this.workloads.stream().map(WorkloadBoundary::workloadId).toList(),
                "duplicate workloadId");
        requireUnique(this.workloads.stream().map(WorkloadBoundary::workloadIdentity).toList(),
                "duplicate workloadIdentity");
        requireUnique(this.workloads.stream().map(WorkloadBoundary::consumerGroup)
                        .filter(Objects::nonNull).toList(),
                "duplicate consumerGroup");

        String calculated = calculatedSha256();
        if (claimedManifestSha256 != null
                && !calculated.equals(requireSha(claimedManifestSha256,
                "claimedManifestSha256"))) {
            throw new IllegalArgumentException("deployment manifest digest mismatch");
        }
        this.manifestSha256 = calculated;
    }

    public static DeploymentBoundaryManifest seal(String deploymentRef, Stage stage,
            String tenantScope, String region, String candidateRef, String candidateSha256,
            String imageRef, String configurationSha256, List<WorkloadBoundary> workloads) {
        return new DeploymentBoundaryManifest(deploymentRef, stage, tenantScope, region,
                candidateRef, candidateSha256, imageRef, configurationSha256, workloads, null);
    }

    public static DeploymentBoundaryManifest verify(String deploymentRef, Stage stage,
            String tenantScope, String region, String candidateRef, String candidateSha256,
            String imageRef, String configurationSha256, List<WorkloadBoundary> workloads,
            String claimedManifestSha256) {
        return new DeploymentBoundaryManifest(deploymentRef, stage, tenantScope, region,
                candidateRef, candidateSha256, imageRef, configurationSha256, workloads,
                claimedManifestSha256);
    }

    public String deploymentRef() { return deploymentRef; }
    public Stage stage() { return stage; }
    public String tenantScope() { return tenantScope; }
    public String region() { return region; }
    public String candidateRef() { return candidateRef; }
    public String candidateSha256() { return candidateSha256; }
    public String imageRef() { return imageRef; }
    public String configurationSha256() { return configurationSha256; }
    public List<WorkloadBoundary> workloads() { return workloads; }
    public String manifestSha256() { return manifestSha256; }

    public String calculatedSha256() {
        List<Object> fields = new ArrayList<>(List.of(deploymentRef, stage, tenantScope, region,
                candidateRef, candidateSha256, imageRef, configurationSha256,
                workloads.size()));
        workloads.stream().sorted(Comparator.comparing(WorkloadBoundary::workloadId))
                .forEach(workload -> {
                    fields.add(workload.workloadId());
                    fields.add(workload.role());
                    fields.add(workload.operationalOwner());
                    fields.add(workload.workloadIdentity());
                    fields.add(workload.stateOwner());
                    fields.add(workload.consumerGroup());
                    fields.add(workload.stateAccess().size());
                    workload.stateAccess().stream()
                            .sorted(Comparator.comparing(StateAccess::storeRef)
                                    .thenComparing(item -> item.mode().name()))
                            .forEach(access -> {
                                fields.add(access.storeRef());
                                fields.add(access.storeClass());
                                fields.add(access.mode());
                            });
                    fields.add(workload.scale().ownedKafkaPartitions());
                    fields.add(workload.scale().minimumReplicas());
                    fields.add(workload.scale().maximumReplicas());
                    fields.add(workload.scale().maxInFlightPerReplica());
                    fields.add(workload.lifecycle().startupTimeoutSeconds());
                    fields.add(workload.lifecycle().drainTimeoutSeconds());
                    fields.add(workload.lifecycle().terminationGraceSeconds());
                    fields.add(workload.lifecycle().withdrawReadinessBeforeDrain());
                    fields.add(workload.lifecycle().gracefulShutdown());
                    addEnums(fields, workload.capabilities());
                    addEnums(fields, workload.credentials());
                    addEnums(fields, workload.publishRoutes());
                    addEnums(fields, workload.networkTargets());
                });
        return canonicalSha256(fields);
    }

    /**
     * One deployable workload may host several accepted logical components, but every field below
     * names an operationally enforceable boundary and is part of the sealed manifest digest.
     */
    // tag::effect-free-analysis-workers[]
    public record WorkloadBoundary(
            String workloadId,
            WorkloadRole role,
            String operationalOwner,
            String workloadIdentity,
            String stateOwner,
            String consumerGroup,
            Set<StateAccess> stateAccess,
            ScaleBoundary scale,
            LifecycleBoundary lifecycle,
            Set<Capability> capabilities,
            Set<Credential> credentials,
            Set<PublishRoute> publishRoutes,
            Set<NetworkTarget> networkTargets) {

        public WorkloadBoundary {
            requireText(workloadId, "workloadId");
            Objects.requireNonNull(role, "role");
            requireText(operationalOwner, "operationalOwner");
            requireText(workloadIdentity, "workloadIdentity");
            requireText(stateOwner, "stateOwner");
            if (consumerGroup != null) requireText(consumerGroup, "consumerGroup");
            stateAccess = Set.copyOf(stateAccess);
            if (stateAccess.isEmpty()) throw new IllegalArgumentException("stateAccess");
            requireUnique(stateAccess.stream().map(StateAccess::storeRef).toList(),
                    "duplicate state store");
            Objects.requireNonNull(scale, "scale");
            Objects.requireNonNull(lifecycle, "lifecycle");
            capabilities = Set.copyOf(capabilities);
            credentials = Set.copyOf(credentials);
            publishRoutes = Set.copyOf(publishRoutes);
            networkTargets = Set.copyOf(networkTargets);

            if (role == WorkloadRole.EVALUATION || role == WorkloadRole.REPLAY
                    || role == WorkloadRole.ANALYSIS) {
                if (capabilities.stream().anyMatch(Capability::consequential))
                    throw new IllegalArgumentException(role + " workload has effect capability");
                if (capabilities.stream().anyMatch(Capability::protectedMutation))
                    throw new IllegalArgumentException(role + " workload has protected mutation capability");
                if (capabilities.stream().anyMatch(capability ->
                        !capability.permittedForAnalysis(role)))
                    throw new IllegalArgumentException(role + " workload has operational capability");
                if (stateAccess.stream().anyMatch(StateAccess::protectedMutation))
                    throw new IllegalArgumentException(role + " workload has protected state mutation");
                if (stateAccess.stream().anyMatch(access -> access.mode() == AccessMode.READ_WRITE
                        && access.storeClass() != StoreClass.ANALYSIS_RESULTS))
                    throw new IllegalArgumentException(role + " workload writes outside analysis results");
                if (credentials.stream().anyMatch(Credential::protectedOperationalState))
                    throw new IllegalArgumentException(role + " workload has protected state credential");
                if (credentials.contains(Credential.EFFECT_TARGET)
                        || credentials.contains(Credential.EFFECT_OBSERVER))
                    throw new IllegalArgumentException(role + " workload has effect credential");
                if (credentials.stream().anyMatch(credential ->
                        !credential.permittedForAnalysis()))
                    throw new IllegalArgumentException(role + " workload has operational credential");
                if (publishRoutes.stream().anyMatch(PublishRoute::protectedMutationRoute))
                    throw new IllegalArgumentException(role + " workload has protected mutation publisher");
                if (publishRoutes.stream().anyMatch(route ->
                        !route.permittedForAnalysis(role)))
                    throw new IllegalArgumentException(role + " workload has operational publisher");
                if (networkTargets.stream().anyMatch(NetworkTarget::protectedOperationalState))
                    throw new IllegalArgumentException(role + " workload has protected state network path");
                if (networkTargets.contains(NetworkTarget.EFFECT_TARGET)
                        || networkTargets.contains(NetworkTarget.EFFECT_OBSERVATION))
                    throw new IllegalArgumentException(role + " workload has effect network path");
                if (networkTargets.stream().anyMatch(target ->
                        !target.permittedForAnalysis()))
                    throw new IllegalArgumentException(role + " workload has operational network path");
            }
            if (role == WorkloadRole.RECOVERY) {
                if (capabilities.contains(Capability.EXECUTE_EFFECT))
                    throw new IllegalArgumentException("RECOVERY workload has mutation capability");
                if (credentials.contains(Credential.EFFECT_TARGET))
                    throw new IllegalArgumentException("RECOVERY workload has mutation credential");
                if (publishRoutes.contains(PublishRoute.EFFECT_COMMANDS))
                    throw new IllegalArgumentException("RECOVERY workload has mutation publisher");
                if (networkTargets.contains(NetworkTarget.EFFECT_TARGET))
                    throw new IllegalArgumentException("RECOVERY workload has mutation network path");
            }
        }
    }
    // end::effect-free-analysis-workers[]

    public record StateAccess(String storeRef, StoreClass storeClass, AccessMode mode) {
        public StateAccess {
            requireText(storeRef, "storeRef");
            Objects.requireNonNull(storeClass, "storeClass");
            Objects.requireNonNull(mode, "mode");
        }

        boolean protectedMutation() {
            return mode == AccessMode.READ_WRITE && storeClass.protectedOperationalState();
        }
    }

    public record ScaleBoundary(int ownedKafkaPartitions, int minimumReplicas,
            int maximumReplicas, int maxInFlightPerReplica) {
        public ScaleBoundary {
            if (ownedKafkaPartitions < 0)
                throw new IllegalArgumentException("ownedKafkaPartitions");
            if (minimumReplicas < 1 || maximumReplicas < minimumReplicas)
                throw new IllegalArgumentException("replicas");
            if (ownedKafkaPartitions > 0 && maximumReplicas > ownedKafkaPartitions)
                throw new IllegalArgumentException("replicas exceed partition ownership");
            if (maxInFlightPerReplica < 1)
                throw new IllegalArgumentException("maxInFlightPerReplica");
        }
    }

    public record LifecycleBoundary(int startupTimeoutSeconds, int drainTimeoutSeconds,
            int terminationGraceSeconds, boolean withdrawReadinessBeforeDrain,
            boolean gracefulShutdown) {
        public LifecycleBoundary {
            if (startupTimeoutSeconds < 1) throw new IllegalArgumentException("startupTimeoutSeconds");
            if (drainTimeoutSeconds < 1) throw new IllegalArgumentException("drainTimeoutSeconds");
            if (terminationGraceSeconds < drainTimeoutSeconds)
                throw new IllegalArgumentException("terminationGraceSeconds");
            if (!withdrawReadinessBeforeDrain)
                throw new IllegalArgumentException("withdrawReadinessBeforeDrain");
            if (!gracefulShutdown) throw new IllegalArgumentException("gracefulShutdown");
        }
    }

    public enum Stage { UNDEPLOYED, DEVELOPMENT, EVALUATION, STAGING, CANARY, PRODUCTION }

    public enum WorkloadRole {
        ADMISSION, INVESTIGATION, READ_GATEWAY, CONTROL, EFFECT_EXECUTION, RECOVERY,
        REPLAY, EVALUATION, ANALYSIS, OPERATIONAL_VIEW
    }

    public enum AccessMode { READ_ONLY, READ_WRITE }

    public enum StoreClass {
        COMMAND_INBOX(true), PROCESS_STATE(true), APPROVAL_STATE(true), EFFECT_LEDGER(true),
        ARTIFACTS(false), RETAINED_HISTORY(false), EVALUATION_CORPUS(false),
        ANALYSIS_RESULTS(false), OPERATIONAL_VIEW(false);

        private final boolean protectedOperationalState;

        StoreClass(boolean protectedOperationalState) {
            this.protectedOperationalState = protectedOperationalState;
        }

        boolean protectedOperationalState() { return protectedOperationalState; }
    }

    public enum Capability {
        ADMIT_COMMAND(false, true), INVESTIGATE(false, false), RESOLVE_CONTEXT(false, false),
        MANAGE_PROCESS(false, true), HANDLE_APPROVAL(false, true),
        READ_HISTORY(false, false), REPLAY_HISTORY(false, false),
        EVALUATE_CANDIDATE(false, false), READ_OPERATIONAL_VIEW(false, false),
        EXECUTE_EFFECT(true, true), RECONCILE_EFFECT(true, true);

        private final boolean consequential;
        private final boolean protectedMutation;

        Capability(boolean consequential, boolean protectedMutation) {
            this.consequential = consequential;
            this.protectedMutation = protectedMutation;
        }

        boolean consequential() { return consequential; }
        boolean protectedMutation() { return protectedMutation; }

        boolean permittedForAnalysis(WorkloadRole role) {
            return this == READ_HISTORY
                    || ((role == WorkloadRole.REPLAY || role == WorkloadRole.ANALYSIS)
                    && this == REPLAY_HISTORY)
                    || ((role == WorkloadRole.EVALUATION || role == WorkloadRole.ANALYSIS)
                    && this == EVALUATE_CANDIDATE);
        }
    }

    public enum Credential {
        BROKER(false), PROCESS_STORE(true), ARTIFACT_STORE(false), SOURCE_SYSTEM(false),
        MODEL_PROVIDER(false), APPROVAL_STORE(true), EVALUATION_STORE(false),
        EFFECT_TARGET(false), EFFECT_OBSERVER(false), TELEMETRY(false);

        private final boolean protectedOperationalState;

        Credential(boolean protectedOperationalState) {
            this.protectedOperationalState = protectedOperationalState;
        }

        boolean protectedOperationalState() { return protectedOperationalState; }

        boolean permittedForAnalysis() {
            return this == BROKER || this == ARTIFACT_STORE || this == MODEL_PROVIDER
                    || this == EVALUATION_STORE || this == TELEMETRY;
        }
    }

    public enum PublishRoute {
        INVESTIGATION_WORK(false), LIFECYCLE_EVENTS(false), APPROVAL_EVENTS(true),
        REPLAY_RESULTS(false), EVALUATION_RESULTS(false), EFFECT_COMMANDS(true),
        EFFECT_OUTCOMES(true);

        private final boolean protectedMutationRoute;

        PublishRoute(boolean protectedMutationRoute) {
            this.protectedMutationRoute = protectedMutationRoute;
        }

        boolean protectedMutationRoute() { return protectedMutationRoute; }

        boolean permittedForAnalysis(WorkloadRole role) {
            return ((role == WorkloadRole.REPLAY || role == WorkloadRole.ANALYSIS)
                    && this == REPLAY_RESULTS)
                    || ((role == WorkloadRole.EVALUATION || role == WorkloadRole.ANALYSIS)
                    && this == EVALUATION_RESULTS);
        }
    }

    public enum NetworkTarget {
        OPERATIONAL_BROKER, EVALUATION_BROKER, PROCESS_STORE, ARTIFACT_STORE,
        READ_SOURCE, MODEL_PROVIDER, APPROVAL_STORE, EVALUATION_STORE, EFFECT_TARGET,
        EFFECT_OBSERVATION, TELEMETRY_COLLECTOR;

        boolean protectedOperationalState() {
            return this == PROCESS_STORE || this == APPROVAL_STORE;
        }

        boolean permittedForAnalysis() {
            return this == EVALUATION_BROKER || this == ARTIFACT_STORE
                    || this == MODEL_PROVIDER || this == EVALUATION_STORE
                    || this == TELEMETRY_COLLECTOR;
        }
    }

    private static void addEnums(List<Object> fields, Set<? extends Enum<?>> values) {
        fields.add(values.size());
        values.stream().map(Enum::name).sorted().forEach(fields::add);
    }

    private static void requireUnique(List<String> values, String message) {
        if (new HashSet<>(values).size() != values.size())
            throw new IllegalArgumentException(message);
    }

    private static String canonicalSha256(List<?> fields) {
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
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(bytes.toByteArray()));
        } catch (IOException | NoSuchAlgorithmException impossible) {
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

    private static String requireImageDigest(String value) {
        if (value == null || !value.matches("[^@\\s]+@sha256:[0-9a-f]{64}"))
            throw new IllegalArgumentException("imageRef");
        return value;
    }
}
// end::sealed-deployment-boundaries[]
