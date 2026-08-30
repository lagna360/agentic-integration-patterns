package dev.agenticintegrationpatterns.orderdesk.failure;

import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;
import java.util.List;

import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.RetryEligibility.*;
import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.RunConsequence.*;
import static dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision.StopCause.DEADLINE;
import static dev.agenticintegrationpatterns.orderdesk.failure.ClassificationObservation.Kind.*;
import static dev.agenticintegrationpatterns.orderdesk.failure.ClassificationObservation.Stage.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class FailureTaxonomyTest {
    private static final Instant NOW = Instant.parse("2026-08-26T14:00:00Z");
    private static final List<String> OUTPUTS = List.of(
            "seda:failure-rejected", "seda:failure-denied",
            "seda:failure-retry-eligible", "seda:investigation-stopped",
            "seda:business-outcome", "seda:effect-reconciliation");

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;
    @Autowired FailureClassifier classifier;
    @Autowired CamelContext camelContext;

    @BeforeEach
    void drainOutputs() {
        OUTPUTS.forEach(this::drain);
    }

    @Test
    // tag::failure-route-tests[]
    void knownFailuresReachDifferentFixedOperationalDestinations() {
        assertRouted(observation("invalid-1", CAPABILITY_GATEWAY, INVALID_WORK,
                "ARGUMENT_SCHEMA_INVALID", "tool-request-sha256:111"),
                "seda:failure-rejected", REJECTED, NEVER_SAME_INPUT);
        assertRouted(observation("result-1", CAPABILITY_GATEWAY, RESULT_CONTRACT_INVALID,
                "RESULT_SCHEMA_INVALID", "result-sha256:112"),
                "seda:failure-rejected", REJECTED, GOVERNED_REPAIR_MAY_SUCCEED);
        assertRouted(observation("denied-1", CAPABILITY_GATEWAY, POLICY_DENIED,
                "OBJECT_SCOPE_DENIED", "policy-decision:222"),
                "seda:failure-denied", DENIED, NEW_AUTHORIZATION_OR_POLICY_REQUIRED);
        assertRouted(observation("dependency-1", CONTEXT_ENRICHMENT, DEPENDENCY_UNAVAILABLE,
                "INVENTORY_UNAVAILABLE", "adapter-observation:333"),
                "seda:failure-retry-eligible", RETRY_ELIGIBLE,
                GOVERNED_RETRY_MAY_SUCCEED);
    }

    @Test
    void deadlineStopsButInsufficientEvidenceCompletesWithABusinessOutcome() {
        var stopped = classifier.classify(observation(
                "deadline-1", PROCESS_MANAGER, DEADLINE_EXCEEDED,
                "ABSOLUTE_DEADLINE", "timer-observation:444"));
        assertThat(stopped.runConsequence()).isEqualTo(STOP);
        assertThat(stopped.stopCause()).isEqualTo(DEADLINE);
        assertThat(stopped.operatorAction()).isEqualTo(
                FailureDecision.OperatorAction.REVIEW_STOPPED_RUN);
        assertThat(stopped.businessOutcome()).isNull();

        var incomplete = classifier.classify(observation(
                "evidence-1", EVIDENCE_AGGREGATION, INSUFFICIENT_EVIDENCE,
                "REQUIRED_SOURCE_MISSING", "evidence-set:555"));
        assertThat(incomplete.runConsequence()).isEqualTo(COMPLETE);
        assertThat(incomplete.businessOutcome()).isEqualTo(
                FailureDecision.BusinessOutcome.INSUFFICIENT_EVIDENCE);
        assertThat(incomplete.stopCause()).isNull();
        assertThat(incomplete.disposition()).isEqualTo(BUSINESS_OUTCOME);
    }

    @Test
    void unknownExternalOutcomeRequiresReconciliationAndNeverBecomesRetryEligible() {
        var decision = classifier.classify(observation(
                "effect-1", EXTERNAL_EFFECT, EXTERNAL_OUTCOME_UNKNOWN,
                "REPLY_TIMED_OUT", "effect-attempt:666"));

        assertThat(decision.disposition()).isEqualTo(RECONCILIATION_REQUIRED);
        assertThat(decision.runConsequence()).isEqualTo(KEEP_RUNNING);
        assertThat(decision.retryEligibility()).isEqualTo(RECONCILE_BEFORE_RETRY);

        producer.sendBody("direct:classify-known-failure", observation(
                "effect-1", EXTERNAL_EFFECT, EXTERNAL_OUTCOME_UNKNOWN,
                "REPLY_TIMED_OUT", "effect-attempt:666"));
        assertThat(consumer.receive("seda:effect-reconciliation", 2_000)).isNotNull();
        assertThat(consumer.receive("seda:failure-retry-eligible", 100)).isNull();
    }
    // end::failure-route-tests[]

    @Test
    void invalidKindStagePairsAndUnboundedTextFailBeforeClassification() {
        assertThatThrownBy(() -> observation(
                "effect-wrong-stage", CAPABILITY_GATEWAY, EXTERNAL_OUTCOME_UNKNOWN,
                "REPLY_TIMED_OUT", "effect-attempt:777"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> observation(
                "bad\n" + "x".repeat(200), CAPABILITY_GATEWAY, INVALID_WORK,
                "ARGUMENT_SCHEMA_INVALID", "tool-request-sha256:888"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unexpectedProgrammingDefectPropagatesInsteadOfBecomingDependencyFailure() {
        FailureClassifier defective = ignored -> {
            throw new NullPointerException("fixture classifier defect");
        };
        var processor = new FailureClassificationProcessor(defective);
        var exchange = new DefaultExchange(camelContext);
        exchange.getMessage().setBody(observation(
                "dependency-2", CONTEXT_ENRICHMENT, DEPENDENCY_UNAVAILABLE,
                "INVENTORY_UNAVAILABLE", "adapter-observation:999"));

        assertThatThrownBy(() -> processor.process(exchange))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fixture classifier defect");
    }

    private void assertRouted(
            ClassificationObservation observation,
            String endpoint,
            FailureDecision.Disposition disposition,
            FailureDecision.RetryEligibility retryEligibility) {
        producer.sendBody("direct:classify-known-failure", observation);
        var exchange = consumer.receive(endpoint, 2_000);
        assertThat(exchange).isNotNull();
        assertThat(exchange.getMessage().getBody(FailureDecision.class)).satisfies(decision -> {
            assertThat(decision.disposition()).isEqualTo(disposition);
            assertThat(decision.retryEligibility()).isEqualTo(retryEligibility);
            assertThat(decision.tenantId()).isEqualTo("tenant-ca");
            assertThat(decision.runId()).isEqualTo("run-12");
            assertThat(decision.taxonomyVersion()).isEqualTo("orderdesk-failure-v1");
            assertThat(decision.stage()).isEqualTo(observation.stage());
            assertThat(decision.observedAt()).isEqualTo(NOW);
        });
    }

    private ClassificationObservation observation(
            String id,
            ClassificationObservation.Stage stage,
            ClassificationObservation.Kind kind,
            String reason,
            String evidenceRef) {
        return new ClassificationObservation(
                "tenant-ca", "run-12", id, stage, kind, reason, evidenceRef, NOW);
    }

    private void drain(String endpoint) {
        while (consumer.receive(endpoint, 25) != null) {
            // Drain asynchronous teaching outputs left by earlier test methods.
        }
    }
}
