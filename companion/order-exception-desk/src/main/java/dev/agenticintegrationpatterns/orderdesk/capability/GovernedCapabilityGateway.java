package dev.agenticintegrationpatterns.orderdesk.capability;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import dev.agenticintegrationpatterns.orderdesk.context.AdmittedWorkFingerprint;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.regex.Pattern;

import static dev.agenticintegrationpatterns.orderdesk.capability.CapabilityGatewayException.Reason.*;

@Component
public final class GovernedCapabilityGateway {
    public static final String CAPABILITY_NAME = "inventory.availability.read.v1";
    public static final String REQUIRED_GRANT = "read-inventory";
    private static final Pattern CALL_ID = Pattern.compile("[A-Za-z0-9._:-]{1,128}");

    private final InventoryArgumentSchemaValidator schemaValidator;
    private final InventoryAvailabilityClient client;
    private final CapabilityInvocationRecorder recorder;
    private final ObjectMapper mapper;
    private final Clock clock;

    public GovernedCapabilityGateway(
            InventoryArgumentSchemaValidator schemaValidator,
            InventoryAvailabilityClient client,
            CapabilityInvocationRecorder recorder,
            ObjectMapper mapper,
            Clock clock) {
        this.schemaValidator = schemaValidator;
        this.client = client;
        this.recorder = recorder;
        this.mapper = mapper;
        this.clock = clock;
    }

    // tag::governed-invocation[]
    public CapabilityEvidence invoke(CapabilityInvocationRequest request) {
        Instant now = clock.instant();
        try {
            validateRequest(request, now);
            var intent = request.intent();
            schemaValidator.validate(intent.arguments());
            var arguments = new InventoryAvailabilityArguments(
                    intent.arguments().get("sku").asString(),
                    intent.arguments().get("locationId").asString());
            var expectedEvidence = authorizeObjectScope(request, arguments);

            InventoryAvailabilityObservation observation;
            try {
                observation = client.read(
                        request.context().snapshot().tenantId(), arguments);
            } catch (InventoryDependencyUnavailableException upstream) {
                throw new CapabilityGatewayException(
                        UPSTREAM_UNAVAILABLE, "Inventory dependency is unavailable");
            }
            validateResult(request, arguments, expectedEvidence, observation, now);

            byte[] argumentBytes = mapper.writeValueAsBytes(arguments);
            byte[] resultBytes = mapper.writeValueAsBytes(observation);
            String resultHash = CapabilityDigests.sha256(resultBytes);
            var evidence = new CapabilityEvidence(
                    stableId("tool-evidence", request.context().snapshot().tenantId(),
                            request.context().snapshot().runId(), intent.callId(), resultHash),
                    request.context().snapshot().runId(),
                    request.context().snapshot().tenantId(), intent.callId(), CAPABILITY_NAME,
                    request.context().snapshot().capabilityCatalogRef(),
                    InventoryArgumentSchemaValidator.SCHEMA_ID,
                    CapabilityDigests.sha256(argumentBytes), resultHash, now, observation);
            recorder.success(evidence);
            return evidence;
        } catch (CapabilityGatewayException failure) {
            recorder.failure(request, failure.reason(), now);
            throw failure;
        } catch (JacksonException failure) {
            // Encoding a validated typed result is an internal defect, not a tool outcome.
            throw new IllegalStateException("Capability evidence encoding failed", failure);
        }
    }
    // end::governed-invocation[]

    private void validateRequest(CapabilityInvocationRequest request, Instant now) {
        if (request == null || request.context() == null
                || request.context().admitted() == null
                || request.context().snapshot() == null
                || request.intent() == null
                || request.intent().callId() == null
                || !CALL_ID.matcher(request.intent().callId()).matches()
                || blank(request.intent().capabilityName())
                || request.completedToolRequests() < 0) {
            throw new CapabilityGatewayException(INVALID_INTENT,
                    "Trusted run context and a correlated tool intent are required");
        }
        var admitted = request.context().admitted();
        if (request.context().modelContext() == null
                || !request.context().snapshot().tenantId()
                        .equals(admitted.trustedContext().tenantId())
                || !request.context().snapshot().tenantId()
                        .equals(admitted.command().tenantId())
                || !request.context().snapshot().snapshotId()
                        .equals(request.context().modelContext().snapshotId())
                || !request.context().snapshot().admittedWorkFingerprint()
                        .equals(AdmittedWorkFingerprint.compute(mapper, admitted))) {
            throw new CapabilityGatewayException(INVALID_INTENT,
                    "Snapshot, model projection, and admitted identity are inconsistent");
        }
        if (!CAPABILITY_NAME.equals(request.intent().capabilityName())) {
            throw new CapabilityGatewayException(UNKNOWN_CAPABILITY,
                    "Capability is not registered in this application catalog");
        }
        if (!admitted.effectiveCapabilities().contains(REQUIRED_GRANT)) {
            throw new CapabilityGatewayException(CAPABILITY_DENIED,
                    "Effective run grants do not permit this capability");
        }
        if (admitted.command().deadlineAt() == null
                || !admitted.command().deadlineAt().isAfter(now)) {
            throw new CapabilityGatewayException(DEADLINE_EXCEEDED,
                    "The admitted run deadline has elapsed");
        }
        if (request.completedToolRequests() >= admitted.effectiveLimits().maxToolRequests()) {
            throw new CapabilityGatewayException(TOOL_BUDGET_EXHAUSTED,
                    "The effective tool-request budget is exhausted");
        }
    }

    private static InvestigateOrderException.EvidenceReference authorizeObjectScope(
            CapabilityInvocationRequest request,
            InventoryAvailabilityArguments arguments) {
        String expectedReference = "inventory://" + arguments.locationId()
                + "/" + arguments.sku() + "@";
        return request.context().admitted().command().evidence().stream()
                .filter(reference -> "inventory-ledger".equals(reference.sourceSystem()))
                .filter(reference -> reference.reference().equals(
                        expectedReference + reference.sourceVersion()))
                .findFirst()
                .orElseThrow(() -> new CapabilityGatewayException(
                        OBJECT_SCOPE_DENIED,
                        "Requested inventory object is outside admitted evidence scope"));
    }

    private static void validateResult(
            CapabilityInvocationRequest request,
            InventoryAvailabilityArguments arguments,
            InvestigateOrderException.EvidenceReference expected,
            InventoryAvailabilityObservation actual,
            Instant now) {
        if (actual == null
                || !request.context().snapshot().tenantId().equals(actual.tenantId())
                || !arguments.sku().equals(actual.sku())
                || !arguments.locationId().equals(actual.locationId())
                || actual.availableUnits() < 0
                || !expected.sourceVersion().equals(actual.sourceVersion())
                || actual.observedAt() == null || actual.observedAt().isAfter(now)
                || actual.validUntil() == null || !actual.validUntil().isAfter(now)) {
            throw new CapabilityGatewayException(RESULT_INVALID,
                    "Capability result violates identity, version, time, or domain constraints");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static String stableId(String type, String... parts) {
        var material = new StringBuilder(type);
        for (String part : parts) {
            material.append('|').append(part.length()).append(':').append(part);
        }
        return type + "-" + UUID.nameUUIDFromBytes(
                material.toString().getBytes(StandardCharsets.UTF_8));
    }
}
