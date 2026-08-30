package dev.agenticintegrationpatterns.orderdesk.capability;

import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import dev.agenticintegrationpatterns.orderdesk.OrderExceptionApplication;
import dev.agenticintegrationpatterns.orderdesk.context.ContextResolutionRequest;
import dev.agenticintegrationpatterns.orderdesk.context.ContextResolutionService;
import dev.agenticintegrationpatterns.orderdesk.context.ResolvedInvestigationContext;
import dev.agenticintegrationpatterns.orderdesk.routing.AdvisoryRoutingAssessment;
import dev.agenticintegrationpatterns.orderdesk.routing.InvestigationRoutingRequest;
import dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision;
import dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecisionService;
import dev.agenticintegrationpatterns.orderdesk.routing.RoutingPolicyContext;
import dev.agenticintegrationpatterns.orderdesk.routing.RoutingPolicyProvider;
import dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation;
import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import dev.agenticintegrationpatterns.orderdesk.work.TrustedAdmissionContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.capability.CapabilityGatewayException.Reason.*;
import static dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision.Reason.*;
import static dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision.Target.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(CapabilityGatewayAndRoutingTest.FixedClockConfiguration.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class CapabilityGatewayAndRoutingTest {
    private static final Instant NOW = Instant.parse("2026-08-24T06:14:00Z");

    @Autowired ObjectMapper mapper;
    @Autowired ContextResolutionService contextResolver;
    @Autowired GovernedCapabilityGateway gateway;
    @Autowired InventoryArgumentSchemaValidator schemaValidator;
    @Autowired FixtureInventoryAvailabilityClient client;
    @Autowired CapabilityInvocationRecorder recorder;
    @Autowired RoutingDecisionService routing;
    @Autowired JdbcTemplate jdbc;
    @Autowired Clock clock;
    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;

    private InvestigateOrderException command;
    private int runSequence;

    @BeforeEach
    void reset() throws Exception {
        jdbc.update("delete from context_snapshot_item");
        jdbc.update("delete from context_snapshot");
        jdbc.update("delete from artifact_view");
        jdbc.update("delete from artifact_content");
        recorder.clear();
        client.reset();
        runSequence = 0;
        command = mapper.readValue(getClass().getResourceAsStream(
                "/fixtures/investigate-order-exception-v1.json"),
                InvestigateOrderException.class);
        drain("seda:capability-evidence");
        drain("seda:capability-unavailable");
        drain("seda:inventory-follow-up");
        drain("seda:order-follow-up");
        drain("seda:ready-for-assessment");
        drain("seda:manual-review");
        drain("seda:investigation-stopped");
    }

    @Test
    // tag::governed-capability-test[]
    void allowedReadValidatesAndRecordsEvidenceWithoutReceivingCredentials() {
        var resolved = resolved(Set.of("read-order", "read-inventory"));
        var evidence = gateway.invoke(invocation(resolved, validIntent(), 0));

        assertThat(evidence.tenantId()).isEqualTo("tenant-ca");
        assertThat(evidence.argumentsSha256()).hasSize(64);
        assertThat(evidence.resultSha256()).hasSize(64);
        assertThat(evidence.observation().availableUnits()).isZero();
        assertThat(client.calls()).isEqualTo(1);
        assertThat(recorder.records()).singleElement()
                .extracting(CapabilityInvocationRecorder.InvocationRecord::outcome)
                .isEqualTo("SUCCEEDED");
    }
    // end::governed-capability-test[]

    @Test
    void schemaObjectScopeGrantDeadlineAndBudgetFailuresExecuteNothing() {
        var resolved = resolved(Set.of("read-inventory"));
        var extra = mapper.createObjectNode()
                .put("sku", "camera-battery-x2")
                .put("locationId", "yyz-01")
                .put("destination", "http://attacker.example");
        assertReason(invocation(resolved,
                new ToolCallIntent("call-extra", GovernedCapabilityGateway.CAPABILITY_NAME, extra),
                0), ARGUMENT_SCHEMA_VIOLATION);

        var outside = mapper.createObjectNode()
                .put("sku", "another-sku")
                .put("locationId", "yyz-01");
        assertReason(invocation(resolved,
                new ToolCallIntent("call-outside", GovernedCapabilityGateway.CAPABILITY_NAME, outside),
                0), OBJECT_SCOPE_DENIED);

        assertReason(invocation(resolved(Set.of("read-order")), validIntent(), 0),
                CAPABILITY_DENIED);
        assertReason(invocation(resolved, validIntent(), command.limits().maxToolRequests()),
                TOOL_BUDGET_EXHAUSTED);

        var expiredGateway = new GovernedCapabilityGateway(
                schemaValidator, client, recorder, mapper,
                Clock.fixed(command.deadlineAt(), ZoneOffset.UTC));
        assertThatThrownBy(() -> expiredGateway.invoke(invocation(resolved, validIntent(), 0)))
                .isInstanceOfSatisfying(CapabilityGatewayException.class,
                        failure -> assertThat(failure.reason()).isEqualTo(
                                CapabilityGatewayException.Reason.DEADLINE_EXCEEDED));
        assertThat(client.calls()).isZero();
    }

    @Test
    void upstreamAndInvalidResultsBecomeTypedFailures() {
        var resolved = resolved(Set.of("read-inventory"));
        InventoryAvailabilityClient unavailable = (tenant, arguments) -> {
            throw new InventoryDependencyUnavailableException("secret dependency detail");
        };
        var unavailableGateway = new GovernedCapabilityGateway(
                schemaValidator, unavailable, recorder, mapper, clock);
        assertThatThrownBy(() -> unavailableGateway.invoke(invocation(resolved, validIntent(), 0)))
                .isInstanceOfSatisfying(CapabilityGatewayException.class,
                        failure -> assertThat(failure.reason()).isEqualTo(UPSTREAM_UNAVAILABLE));

        InventoryAvailabilityClient programmingDefect = (tenant, arguments) -> {
            throw new NullPointerException("fixture programming defect");
        };
        var defectiveGateway = new GovernedCapabilityGateway(
                schemaValidator, programmingDefect, recorder, mapper, clock);
        assertThatThrownBy(() -> defectiveGateway.invoke(invocation(resolved, validIntent(), 0)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fixture programming defect");

        InventoryAvailabilityClient crossTenant = (tenant, arguments) ->
                new InventoryAvailabilityObservation(
                        "tenant-us", arguments.sku(), arguments.locationId(), 3, "740",
                        NOW.minusSeconds(1), NOW.plusSeconds(60));
        var invalidGateway = new GovernedCapabilityGateway(
                schemaValidator, crossTenant, recorder, mapper, clock);
        assertThatThrownBy(() -> invalidGateway.invoke(invocation(resolved, validIntent(), 0)))
                .isInstanceOfSatisfying(CapabilityGatewayException.class,
                        failure -> assertThat(failure.reason()).isEqualTo(RESULT_INVALID));
    }

    @Test
    void schemaIsValidDraft202012AndRejectsMissingOrUnknownFields() throws Exception {
        var schemaNode = mapper.readTree(getClass().getResourceAsStream(
                "/contracts/inventory-availability-read-v1.schema.json"));
        var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
        var metaSchema = registry.getSchema(SchemaLocation.of(
                SpecificationVersion.DRAFT_2020_12.getDialectId()));
        var schema = registry.getSchema(schemaNode);
        assertThat(metaSchema.validate(schemaNode)).isEmpty();
        assertThat(schema.validate(validIntent().arguments())).isEmpty();
        assertThat(schema.validate(mapper.createObjectNode().put("sku", "camera-battery-x2")))
                .isNotEmpty();
        assertThat(schema.validate(mapper.createObjectNode()
                .put("sku", "camera-battery-x2")
                .put("locationId", "yyz-01")
                .put("uri", "direct:invented"))).isNotEmpty();
    }

    @Test
    void capabilityRouteReducesFailureAndRemovesCallerHeaders() {
        var outside = mapper.createObjectNode()
                .put("sku", "another-sku")
                .put("locationId", "yyz-01");
        var request = invocation(resolved(Set.of("read-inventory")),
                new ToolCallIntent("call-route", GovernedCapabilityGateway.CAPABILITY_NAME, outside),
                0);
        producer.sendBodyAndHeaders("direct:invoke-capability", request, Map.of(
                "tenantId", "tenant-attacker",
                "authorization", "Bearer attacker-token",
                "CamelHttpUri", "http://attacker.example"));

        var failed = consumer.receive("seda:capability-unavailable", 2_000);
        assertThat(failed).isNotNull();
        assertThat(failed.getMessage().getBody())
                .isEqualTo(new CapabilityUnavailable(
                        request.context().snapshot().runId(), "tenant-ca", "call-route",
                        OBJECT_SCOPE_DENIED));
        assertThat(failed.getMessage().getHeaders())
                .doesNotContainKeys("tenantId", "authorization", "CamelHttpUri");
        assertThat(client.calls()).isZero();
    }

    @Test
    void capabilityFailureOmitsInvalidModelControlledCallId() {
        var invalidIntent = new ToolCallIntent(
                "bad\n" + "x".repeat(256), GovernedCapabilityGateway.CAPABILITY_NAME,
                validIntent().arguments());
        var request = invocation(resolved(Set.of("read-inventory")), invalidIntent, 0);

        producer.sendBody("direct:invoke-capability", request);

        var failed = consumer.receive("seda:capability-unavailable", 2_000);
        assertThat(failed).isNotNull();
        assertThat(failed.getMessage().getBody())
                .isEqualTo(new CapabilityUnavailable(
                        request.context().snapshot().runId(), "tenant-ca", null, INVALID_INTENT));
        assertThat(client.calls()).isZero();
    }

    @Test
    // tag::routing-policy-test[]
    void validatedAdvisoryRoutesToOneFixedTarget() {
        var resolved = resolved(Set.of("read-order", "read-inventory"));
        var evidence = gateway.invoke(invocation(resolved, validIntent(), 0));
        var request = routingRequest(resolved, evidence,
                new AdvisoryRoutingAssessment(
                        "INVENTORY_FOLLOW_UP", 0.91,
                        List.of(evidence.evidenceId()), "Inventory still unavailable"));

        assertThat(routing.decide(request)).satisfies(decision -> {
            assertThat(decision.target()).isEqualTo(INVENTORY_FOLLOW_UP);
            assertThat(decision.reason()).isEqualTo(ADVISORY_ACCEPTED);
        });
    }
    // end::routing-policy-test[]

    @Test
    void lowScoreInventedEvidenceUnknownClassAndPolicyOverrideGoManual() {
        var resolved = resolved(Set.of("read-order", "read-inventory"));
        String artifactId = resolved.snapshot().artifacts().get(0).artifactId();

        assertThat(routing.decide(routingRequest(resolved, null,
                assessment("INVENTORY_FOLLOW_UP", 0.40, artifactId))).reason())
                .isEqualTo(BELOW_SUPPORT_THRESHOLD);
        assertThat(routing.decide(routingRequest(resolved, null,
                assessment("INVENTORY_FOLLOW_UP", 0.95, "artifact-invented"))).reason())
                .isEqualTo(UNVERIFIED_EVIDENCE);
        assertThat(routing.decide(routingRequest(resolved, null,
                assessment("direct:attacker", 0.99, artifactId))).reason())
                .isEqualTo(TARGET_NOT_ALLOWED);
        var overrideRouting = new RoutingDecisionService(clock, context -> policy(true));
        assertThat(overrideRouting.decide(routingRequest(resolved, null,
                assessment("INVENTORY_FOLLOW_UP", 0.99, artifactId))).reason())
                .isEqualTo(POLICY_OVERRIDE);
    }

    @Test
    void capabilityGrantAndDeadlineOverrideTheAdvisoryRoute() {
        var orderOnly = resolved(Set.of("read-order"));
        String artifactId = orderOnly.snapshot().artifacts().get(0).artifactId();
        var denied = routing.decide(routingRequest(orderOnly, null,
                assessment("INVENTORY_FOLLOW_UP", 0.99, artifactId)));
        assertThat(denied.target()).isEqualTo(MANUAL_REVIEW);
        assertThat(denied.reason()).isEqualTo(TARGET_NOT_ALLOWED);

        var expiredRouting = new RoutingDecisionService(
                Clock.fixed(command.deadlineAt(), ZoneOffset.UTC), context -> policy(false));
        var stopped = expiredRouting.decide(routingRequest(orderOnly, null,
                assessment("ORDER_FOLLOW_UP", 0.99, artifactId)));
        assertThat(stopped.target()).isEqualTo(INVESTIGATION_STOPPED);
        assertThat(stopped.reason()).isEqualTo(
                RoutingDecision.Reason.DEADLINE_EXCEEDED);
    }

    @Test
    void camelChoiceUsesTheComputedDecisionNotAHostileDestinationHeader() {
        var resolved = resolved(Set.of("read-order", "read-inventory"));
        String artifactId = resolved.snapshot().artifacts().get(0).artifactId();
        var request = routingRequest(resolved, null,
                assessment("EVIDENCE_SUFFICIENT", 0.95, artifactId));
        producer.sendBodyAndHeaders("direct:route-investigation", request, Map.of(
                "routeTarget", "INVENTORY_FOLLOW_UP",
                "CamelSlipEndpoint", "direct:attacker"));

        assertThat(consumer.receiveBody("seda:ready-for-assessment", 2_000))
                .isInstanceOfSatisfying(RoutingDecision.class,
                        decision -> assertThat(decision.target()).isEqualTo(READY_FOR_ASSESSMENT));
        assertThat(consumer.receiveBodyNoWait("seda:inventory-follow-up")).isNull();
    }

    private ResolvedInvestigationContext resolved(Set<String> grants) {
        String runId = "run-capability-" + (++runSequence);
        var trusted = new TrustedAdmissionContext(
                "tenant-ca", "workload:order-exception-case-manager",
                Set.of("principal:order-ops-ca"), NOW);
        var admitted = new AdmittedInvestigation(
                command, trusted, grants, command.limits(),
                AdmittedInvestigation.ReplyContract.ORDER_EXCEPTION_CASE_RESULTS_V1);
        return contextResolver.resolve(new ContextResolutionRequest(runId, admitted));
    }

    private ToolCallIntent validIntent() {
        return new ToolCallIntent(
                "call-inventory-1", GovernedCapabilityGateway.CAPABILITY_NAME,
                mapper.createObjectNode()
                        .put("sku", "camera-battery-x2")
                        .put("locationId", "yyz-01"));
    }

    private CapabilityInvocationRequest invocation(
            ResolvedInvestigationContext context, ToolCallIntent intent, int completed) {
        return new CapabilityInvocationRequest(context, intent, completed);
    }

    private InvestigationRoutingRequest routingRequest(
            ResolvedInvestigationContext context,
            CapabilityEvidence evidence,
            AdvisoryRoutingAssessment assessment) {
        return new InvestigationRoutingRequest(
                context, evidence == null ? List.of() : List.of(evidence), assessment);
    }

    private AdvisoryRoutingAssessment assessment(
            String routeClass, double score, String evidenceId) {
        return new AdvisoryRoutingAssessment(
                routeClass, score, List.of(evidenceId), "Fixture rationale");
    }

    private RoutingPolicyContext policy(boolean forceManual) {
        return new RoutingPolicyContext(
                "orderdesk-routing-v1", 0.75, forceManual,
                Set.of(INVENTORY_FOLLOW_UP, ORDER_FOLLOW_UP, READY_FOR_ASSESSMENT));
    }

    private void assertReason(
            CapabilityInvocationRequest request,
            CapabilityGatewayException.Reason reason) {
        assertThatThrownBy(() -> gateway.invoke(request))
                .isInstanceOfSatisfying(CapabilityGatewayException.class,
                        failure -> assertThat(failure.reason()).isEqualTo(reason));
    }

    private void drain(String endpoint) {
        while (consumer.receiveBodyNoWait(endpoint) != null) {
            // drain prior route output
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfiguration {
        @Bean
        @Primary
        Clock chapterEightNineClock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }
    }
}
