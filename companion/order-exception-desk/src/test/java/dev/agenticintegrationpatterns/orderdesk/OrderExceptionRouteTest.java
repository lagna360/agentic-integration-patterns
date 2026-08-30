package dev.agenticintegrationpatterns.orderdesk;

import dev.agenticintegrationpatterns.orderdesk.application.AssessmentGatewayException;
import dev.agenticintegrationpatterns.orderdesk.application.FailureAssessmentGateway;
import dev.agenticintegrationpatterns.orderdesk.application.InMemoryAuditRecorder;
import dev.agenticintegrationpatterns.orderdesk.application.InMemoryCaseStore;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentDisposition;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentProvenance;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import dev.agenticintegrationpatterns.orderdesk.model.FailureAssessment;
import dev.agenticintegrationpatterns.orderdesk.model.GatewayAssessment;
import dev.agenticintegrationpatterns.orderdesk.model.ManualReviewRequired;
import dev.agenticintegrationpatterns.orderdesk.model.ProcessingFailure;
import dev.agenticintegrationpatterns.orderdesk.model.ProposedResolution;
import dev.agenticintegrationpatterns.orderdesk.model.ResolutionProposed;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "orderdesk.routes.proposed-uri=mock:proposed",
        "orderdesk.routes.manual-review-uri=mock:manual",
        "orderdesk.routes.invalid-uri=mock:invalid",
        "orderdesk.routes.assessment-failed-uri=mock:assessment-failed",
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none"
})
class OrderExceptionRouteTest {
    @Autowired
    ProducerTemplate producer;
    @Autowired
    CamelContext camelContext;
    @Autowired
    ScriptedGateway gateway;
    @Autowired
    InMemoryCaseStore caseStore;
    @Autowired
    InMemoryAuditRecorder auditRecorder;
    @Autowired
    Environment environment;

    private String validEvent;

    @BeforeEach
    void reset() throws Exception {
        validEvent = new String(getClass().getResourceAsStream(
                "/fixtures/inventory-shortfall.json").readAllBytes(), StandardCharsets.UTF_8);
        gateway.reset();
        caseStore.reset();
        auditRecorder.reset();
        for (String name : List.of("proposed", "manual", "invalid", "assessment-failed")) {
            endpoint(name).reset();
        }
    }

    @Test
    void proposesResolutionFromVerifiedEvidenceAndPreservesCorrelation() throws Exception {
        gateway.returnAssessment(proposal("inventory://yyz-02/camera-battery-x2@741"));
        endpoint("proposed").expectedMessageCount(1);

        producer.sendBody("direct:inventory-shortfall", validEvent);

        endpoint("proposed").assertIsSatisfied(2_000);
        ResolutionProposed result = endpoint("proposed").getExchanges().get(0)
                .getMessage().getBody(ResolutionProposed.class);
        assertThat(result.eventId()).startsWith("evt-").isNotEqualTo("evt-019482");
        assertThat(result.causedBy()).isEqualTo("evt-019482");
        assertThat(result.correlationId()).isEqualTo("corr-order-73051");
        assertThat(result.tenantId()).isEqualTo("tenant-ca");
        assertThat(result.caseId())
                .isEqualTo("case-d5a30e20-f10b-38ca-9198-4834746bd37b");
        assertThat(result.category()).isEqualTo(ProposedResolution.SPLIT_SHIPMENT);
        assertThat(result.evidenceReferences())
                .containsExactly("inventory://yyz-02/camera-battery-x2@741");
        assertThat(result.provider()).isEqualTo("test");
        assertThat(auditRecorder.records()).hasSize(1);
        assertThat(auditRecorder.records().get(0).provider()).isEqualTo("test");
        assertThat(auditRecorder.records().get(0).instructionVersion()).isEqualTo("test-v1");
    }

    @Test
    void routesAnExplicitAbstentionToManualReview() throws Exception {
        gateway.returnAssessment(manualReview());
        endpoint("manual").expectedMessageCount(1);

        producer.sendBody("direct:inventory-shortfall", validEvent);

        endpoint("manual").assertIsSatisfied(2_000);
        assertThat(endpoint("manual").getExchanges().get(0).getMessage().getBody())
                .isInstanceOf(ManualReviewRequired.class);
    }

    @Test
    void quarantinesTruncatedJsonWithoutCallingTheGateway() throws Exception {
        endpoint("invalid").expectedMessageCount(1);

        producer.sendBody("direct:inventory-shortfall", validEvent.substring(0, 90));

        endpoint("invalid").assertIsSatisfied(2_000);
        assertThat(gateway.callCount()).isZero();
        ProcessingFailure failure = endpoint("invalid").getExchanges().get(0)
                .getMessage().getBody(ProcessingFailure.class);
        assertThat(failure.kind().name()).isEqualTo("INVALID_MESSAGE");
    }

    @Test
    void rejectsImpossibleQuantitiesBeforeAssessment() throws Exception {
        endpoint("invalid").expectedMessageCount(1);
        String invalid = validEvent.replace("\"availableQuantity\": 0", "\"availableQuantity\": -1");

        producer.sendBody("direct:inventory-shortfall", invalid);

        endpoint("invalid").assertIsSatisfied(2_000);
        assertThat(gateway.callCount()).isZero();
    }

    @Test
    void providerFailureIsAttemptedOnceAndBecomesATypedFailure() throws Exception {
        gateway.fail(new AssessmentGatewayException("provider unavailable", new RuntimeException("timeout")));
        endpoint("assessment-failed").expectedMessageCount(1);

        producer.sendBody("direct:inventory-shortfall", validEvent);

        endpoint("assessment-failed").assertIsSatisfied(2_000);
        assertThat(gateway.callCount()).isEqualTo(1);
        ProcessingFailure failure = endpoint("assessment-failed").getExchanges().get(0)
                .getMessage().getBody(ProcessingFailure.class);
        assertThat(failure.kind().name()).isEqualTo("ASSESSMENT_UNAVAILABLE");
        assertThat(failure.eventId()).isEqualTo("evt-019482");
        assertThat(failure.tenantId()).isEqualTo("tenant-ca");
        assertThat(failure.caseId()).isNotBlank();
    }

    @Test
    void rejectsModelEvidenceThatWasNotSupplied() throws Exception {
        gateway.returnAssessment(proposal("inventory://unknown/claim@1"));
        endpoint("assessment-failed").expectedMessageCount(1);

        producer.sendBody("direct:inventory-shortfall", validEvent);

        endpoint("assessment-failed").assertIsSatisfied(2_000);
        ProcessingFailure failure = endpoint("assessment-failed").getExchanges().get(0)
                .getMessage().getBody(ProcessingFailure.class);
        assertThat(failure.kind().name()).isEqualTo("INVALID_ASSESSMENT");
    }

    @Test
    void rejectsMissingAssessmentProvenance() throws Exception {
        gateway.returnAssessment(new GatewayAssessment(
                proposal("inventory://yyz-02/camera-battery-x2@741").assessment(),
                new AssessmentProvenance("openai", " ", "order-exception-assessment-v1")));
        endpoint("assessment-failed").expectedMessageCount(1);

        producer.sendBody("direct:inventory-shortfall", validEvent);

        endpoint("assessment-failed").assertIsSatisfied(2_000);
        assertThat(endpoint("proposed").getExchanges()).isEmpty();
    }

    @Test
    void differentTenantsCannotShareCaseIdentity() throws Exception {
        gateway.returnAssessment(proposal("inventory://yyz-02/camera-battery-x2@741"));
        gateway.returnAssessment(proposal("inventory://yyz-02/camera-battery-x2@741"));
        endpoint("proposed").expectedMessageCount(2);

        producer.sendBody("direct:inventory-shortfall", validEvent);
        producer.sendBody("direct:inventory-shortfall",
                validEvent.replace("\"tenantId\": \"tenant-ca\"", "\"tenantId\": \"tenant-us\""));

        endpoint("proposed").assertIsSatisfied(2_000);
        var first = endpoint("proposed").getExchanges().get(0)
                .getMessage().getBody(ResolutionProposed.class);
        var second = endpoint("proposed").getExchanges().get(1)
                .getMessage().getBody(ResolutionProposed.class);
        assertThat(first.tenantId()).isEqualTo("tenant-ca");
        assertThat(second.tenantId()).isEqualTo("tenant-us");
        assertThat(second.caseId()).isNotEqualTo(first.caseId());
    }

    @Test
    void repeatedEventsReuseTheCaseIdentityAndAdvanceItsVersion() throws Exception {
        gateway.returnAssessment(proposal("inventory://yyz-02/camera-battery-x2@741"));
        gateway.returnAssessment(proposal("inventory://yyz-02/camera-battery-x2@741"));
        endpoint("proposed").expectedMessageCount(2);

        producer.sendBody("direct:inventory-shortfall", validEvent);
        producer.sendBody("direct:inventory-shortfall", validEvent);

        endpoint("proposed").assertIsSatisfied(2_000);
        var first = endpoint("proposed").getExchanges().get(0).getMessage().getBody(ResolutionProposed.class);
        var second = endpoint("proposed").getExchanges().get(1).getMessage().getBody(ResolutionProposed.class);
        assertThat(second.caseId()).isEqualTo(first.caseId());
        assertThat(second.eventId()).isNotEqualTo(first.eventId());
        assertThat(second.causedBy()).isEqualTo(first.causedBy());
        assertThat(gateway.requestVersions()).containsExactly(1L, 2L);
        assertThat(auditRecorder.records()).hasSize(2);
    }

    @Test
    void applicationStartsWithoutAnApiKeyInTheDefaultProfile() {
        assertThat(camelContext.isStarted()).isTrue();
        assertThat(environment.getProperty("spring.ai.model.chat")).isEqualTo("none");
        assertThat(environment.getProperty("spring.ai.model.embedding")).isEqualTo("none");
        assertThat(environment.getProperty("spring.ai.model.image")).isEqualTo("none");
        assertThat(environment.getProperty("spring.ai.model.moderation")).isEqualTo("none");
        assertThat(environment.getProperty("spring.ai.model.audio.speech")).isEqualTo("none");
        assertThat(environment.getProperty("spring.ai.model.audio.transcription")).isEqualTo("none");
        assertThat(environment.matchesProfiles("openai")).isFalse();
    }

    private MockEndpoint endpoint(String name) {
        return camelContext.getEndpoint("mock:" + name, MockEndpoint.class);
    }

    private static GatewayAssessment proposal(String reference) {
        return result(new FailureAssessment(
                AssessmentDisposition.PROPOSE_RESOLUTION,
                ProposedResolution.SPLIT_SHIPMENT,
                "Use verified alternate stock.",
                List.of(reference)));
    }

    private static GatewayAssessment manualReview() {
        return result(new FailureAssessment(
                AssessmentDisposition.REQUEST_MANUAL_REVIEW,
                ProposedResolution.NONE,
                "Evidence is not sufficient.",
                List.of()));
    }

    private static GatewayAssessment result(FailureAssessment assessment) {
        return new GatewayAssessment(
                assessment,
                new AssessmentProvenance("test", "scripted", "test-v1"));
    }

    @TestConfiguration
    static class GatewayTestConfiguration {
        @Bean
        @Primary
        ScriptedGateway scriptedGateway() {
            return new ScriptedGateway();
        }
    }

    static final class ScriptedGateway implements FailureAssessmentGateway {
        private final Queue<Object> script = new ArrayDeque<>();
        private final List<AssessmentRequest> requests = new ArrayList<>();
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public GatewayAssessment assess(AssessmentRequest request) {
            calls.incrementAndGet();
            requests.add(request);
            Object next = script.remove();
            if (next instanceof RuntimeException failure) {
                throw failure;
            }
            return (GatewayAssessment) next;
        }

        void returnAssessment(GatewayAssessment assessment) {
            script.add(assessment);
        }

        void fail(RuntimeException failure) {
            script.add(failure);
        }

        int callCount() {
            return calls.get();
        }

        List<Long> requestVersions() {
            return requests.stream()
                    .map(request -> request.caseWork().orderExceptionCase().version())
                    .toList();
        }

        void reset() {
            script.clear();
            requests.clear();
            calls.set(0);
        }
    }
}
