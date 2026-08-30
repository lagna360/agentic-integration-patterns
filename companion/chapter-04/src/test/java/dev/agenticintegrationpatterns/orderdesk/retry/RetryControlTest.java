package dev.agenticintegrationpatterns.orderdesk.retry;

import com.sun.net.httpserver.HttpServer;
import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
import dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt;
import dev.agenticintegrationpatterns.orderdesk.failure.ClassificationObservation;
import dev.agenticintegrationpatterns.orderdesk.failure.FailureDecision;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static dev.agenticintegrationpatterns.orderdesk.retry.RetryAdmissionResult.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.retry.RetryPolicyDecision.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.retry.RetryScheduleReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.retry.RetryScheduleReceipt.State.CONSUMED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(RetryControlTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class RetryControlTest {
    private static final Instant NOW = Instant.parse("2026-08-26T16:00:00Z");

    @Autowired RetryControlService control;
    @Autowired EstablishedRetryPolicy policy;
    @Autowired JdbcRetrySchedule schedules;
    @Autowired RetryAttemptAdmission admission;
    @Autowired ProducerTemplate producer;
    @Autowired JdbcTemplate jdbc;
    @Autowired AdjustableClock clock;
    @Autowired CircuitBreaker circuitBreaker;

    @BeforeEach
    void reset() {
        jdbc.update("delete from retry_schedule");
        clock.set(NOW);
        circuitBreaker.reset();
    }

    @AfterEach
    void cleanSharedTeachingDatabase() {
        jdbc.update("delete from retry_schedule");
    }

    @Test
    // tag::deadline-and-jitter-test[]
    void equalJitterIsCappedAndATrustedRetryHintCanDelayButNotResetTheDeadline() {
        var deterministic = new EstablishedRetryPolicy(
                Clock.fixed(NOW, ZoneOffset.UTC), new EqualJitterBackoff(() -> 0.0));
        var request = request("retry-jitter", null, NOW.plusSeconds(30),
                NOW.plusSeconds(3), 5, 3, 10_000, 100, 500,
                1_000_000, 10_000, 20_000);

        var eligible = deterministic.decide(request);

        assertThat(eligible.disposition()).isEqualTo(SCHEDULED);
        assertThat(eligible.selectedBackoff()).isEqualTo(Duration.ofSeconds(2));
        assertThat(eligible.notBefore()).isEqualTo(NOW.plusSeconds(3));
        assertThat(eligible.attemptTimeout()).isEqualTo(Duration.ofSeconds(10));

        var shrunk = copyWithDeadline(request, NOW.plusSeconds(12));
        assertThat(deterministic.decide(shrunk).attemptTimeout())
                .isEqualTo(Duration.ofSeconds(7));

        var tooLate = copyWithDeadline(request, NOW.plusSeconds(5));
        assertThat(deterministic.decide(tooLate).disposition())
                .isEqualTo(DEADLINE_EXHAUSTED);
    }
    // end::deadline-and-jitter-test[]

    @Test
    void derivedAttemptTimeoutIsPersistedAndCannotGrowWhenThePermitIsClaimed() {
        var result = control.schedule(request("retry-timeout", null, NOW.plusSeconds(12),
                NOW.plusSeconds(3), 3, 1, 1_000, 0, 10, 10_000, 0, 100));
        assertThat(result.policyDecision().attemptTimeout()).isEqualTo(Duration.ofSeconds(7));
        clock.set(NOW.plusSeconds(3));

        var claim = schedules.claimDue(
                "tenant-ca", "retry-timeout", "timeout-worker", Duration.ofSeconds(5))
                .orElseThrow();

        assertThat(claim.attemptTimeout()).isEqualTo(Duration.ofSeconds(7));
        schedules.consume(claim);
    }

    @Test
    // tag::unsafe-effect-retry-test[]
    void unsafeEffectStatesAndMissingProtectionNeverReceiveAPermit() {
        assertThat(policy.decide(request("retry-unknown",
                effect(EffectReceipt.State.UNKNOWN, NOW.plusSeconds(60)),
                NOW.plusSeconds(30), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100)).disposition())
                .isEqualTo(EFFECT_OUTCOME_UNRESOLVED);
        assertThat(policy.decide(request("retry-accepted",
                effect(EffectReceipt.State.ACCEPTED, NOW.plusSeconds(60)),
                NOW.plusSeconds(30), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100)).disposition())
                .isEqualTo(EFFECT_ALREADY_ACCEPTED);
        assertThat(policy.decide(request("retry-recorded",
                effect(EffectReceipt.State.RECORDED, NOW.plusSeconds(60)),
                NOW.plusSeconds(30), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100)).disposition())
                .isEqualTo(EFFECT_NOT_CONFIRMED_FAILED);
        assertThat(policy.decide(request("retry-expired",
                effect(EffectReceipt.State.FAILED_CONFIRMED, NOW.plusSeconds(2)),
                NOW.plusSeconds(30), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100)).disposition())
                .isEqualTo(IDEMPOTENCY_KEY_EXPIRED);
        assertThat(policy.decide(request("retry-missing-key",
                effect(EffectReceipt.State.FAILED_CONFIRMED, null, NOW.plusSeconds(60)),
                NOW.plusSeconds(30), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100)).disposition())
                .isEqualTo(EFFECT_RETRY_PROTECTION_MISSING);
        assertThat(policy.decide(request("retry-blank-key",
                effect(EffectReceipt.State.FAILED_CONFIRMED, " ", NOW.plusSeconds(60)),
                NOW.plusSeconds(30), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100)).reasonCode())
                .isEqualTo("ORIGINAL_TARGET_IDEMPOTENCY_KEY_MISSING");
        assertThat(policy.decide(request("retry-missing-expiry",
                effect(EffectReceipt.State.FAILED_CONFIRMED, "effect-key-14", null),
                NOW.plusSeconds(30), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100)).reasonCode())
                .isEqualTo("TARGET_IDEMPOTENCY_EXPIRY_MISSING");
        assertThat(jdbc.queryForObject(
                "select count(*) from retry_schedule", Integer.class)).isZero();
    }
    // end::unsafe-effect-retry-test[]

    @Test
    void confirmedNotAppliedEffectWithAProtectiveKeyWindowMayBeScheduled() {
        var result = control.schedule(request("retry-confirmed-failed",
                effect(EffectReceipt.State.FAILED_CONFIRMED, NOW.plusSeconds(60)),
                NOW.plusSeconds(30), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100));

        assertThat(result.policyDecision().disposition()).isEqualTo(SCHEDULED);
        assertThat(result.scheduleReceipt().disposition()).isEqualTo(CREATED);
        assertThat(jdbc.queryForObject(
                "select count(*) from retry_schedule", Integer.class)).isEqualTo(1);
    }

    @Test
    // tag::retry-budget-test[]
    void attemptTokenAndCostBudgetsAreIndependentAdmissionConditions() {
        assertThat(policy.decide(request("retry-attempts", null, NOW.plusSeconds(30),
                null, 2, 2, 100, 0, 1, 100, 0, 1)).disposition())
                .isEqualTo(ATTEMPT_BUDGET_EXHAUSTED);
        assertThat(policy.decide(request("retry-tokens", null, NOW.plusSeconds(30),
                null, 3, 1, 100, 90, 11, 100, 0, 1)).disposition())
                .isEqualTo(TOKEN_BUDGET_EXHAUSTED);
        assertThat(policy.decide(request("retry-cost", null, NOW.plusSeconds(30),
                null, 3, 1, 100, 0, 1, 100, 90, 11)).disposition())
                .isEqualTo(COST_BUDGET_EXHAUSTED);
    }
    // end::retry-budget-test[]

    @Test
    void aNonRetryableFailureAndAnUnexpectedPolicyDefectAreNotConvertedIntoRetries() {
        var nonRetryable = request("retry-denied", null, NOW.plusSeconds(30),
                null, 3, 1, 100, 0, 1, 100, 0, 1, deniedFailure());
        assertThat(control.schedule(nonRetryable).policyDecision().disposition())
                .isEqualTo(FAILURE_NOT_RETRYABLE);

        var brokenPolicy = new EstablishedRetryPolicy(
                Clock.fixed(NOW, ZoneOffset.UTC), new EqualJitterBackoff(() -> 1.0));
        assertThatThrownBy(() -> brokenPolicy.decide(request("retry-defect", null,
                NOW.plusSeconds(30), null, 3, 1,
                100, 0, 1, 100, 0, 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("jitter sample");
        assertThat(jdbc.queryForObject(
                "select count(*) from retry_schedule", Integer.class)).isZero();
    }

    @Test
    // tag::durable-retry-concurrency-test[]
    void duplicateSchedulingIsIdempotentAndConcurrentClaimersReceiveOnePermit() throws Exception {
        var request = request("retry-concurrent", null, NOW.plusSeconds(30),
                null, 3, 1, 1_000, 100, 25, 10_000, 500, 75);
        assertThat(control.schedule(request).scheduleReceipt().disposition()).isEqualTo(CREATED);
        assertThat(control.schedule(request).scheduleReceipt().disposition())
                .isEqualTo(DUPLICATE_SAME);
        var changed = request("retry-concurrent", null, NOW.plusSeconds(30),
                null, 3, 1, 1_000, 100, 26, 10_000, 500, 75);
        assertThat(control.schedule(changed).scheduleReceipt().disposition())
                .isEqualTo(IDENTITY_COLLISION);

        clock.set(NOW.plusSeconds(1));
        var start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            List<java.util.concurrent.Future<java.util.Optional<ClaimedRetry>>> results =
                    new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                int worker = i;
                results.add(pool.submit(() -> {
                    start.await();
                    return schedules.claimDue(
                            "tenant-ca", "retry-concurrent", "worker-" + worker,
                            Duration.ofSeconds(5));
                }));
            }
            start.countDown();
            var claims = List.of(results.get(0).get(), results.get(1).get());
            assertThat(claims.stream().filter(java.util.Optional::isPresent).count())
                    .isEqualTo(1);
            schedules.consume(claims.stream().flatMap(java.util.Optional::stream)
                    .findFirst().orElseThrow());
        } finally {
            pool.shutdownNow();
        }

        var consumed = schedules.current("tenant-ca", "retry-concurrent");
        assertThat(consumed.state()).isEqualTo(CONSUMED);
        assertThat(consumed.attemptsUsed()).isEqualTo(2);
        assertThat(consumed.tokensUsed()).isEqualTo(125);
        assertThat(consumed.costUsedMicros()).isEqualTo(575);
    }
    // end::durable-retry-concurrency-test[]

    @Test
    void changedEffectRetryProtectionCollidesWithAnExistingScheduleIdentity() {
        var original = request("retry-protection-identity",
                effect(EffectReceipt.State.FAILED_CONFIRMED,
                        "effect-key-original", NOW.plusSeconds(60)),
                NOW.plusSeconds(20), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100);
        assertThat(control.schedule(original).scheduleReceipt().disposition())
                .isEqualTo(CREATED);

        var narrowedExpiry = request("retry-protection-identity",
                effect(EffectReceipt.State.FAILED_CONFIRMED,
                        "effect-key-original", NOW.plusSeconds(30)),
                NOW.plusSeconds(20), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100);
        assertThat(control.schedule(narrowedExpiry).scheduleReceipt().disposition())
                .isEqualTo(IDENTITY_COLLISION);

        var changedKey = request("retry-protection-identity",
                effect(EffectReceipt.State.FAILED_CONFIRMED,
                        "effect-key-changed", NOW.plusSeconds(60)),
                NOW.plusSeconds(20), null, 3, 1,
                1_000, 0, 10, 10_000, 0, 100);
        assertThat(control.schedule(changedKey).scheduleReceipt().disposition())
                .isEqualTo(IDENTITY_COLLISION);
    }

    @Test
    void capacityAndCircuitBreakerRefuseWorkBeforeAnotherDurableClaim() {
        control.schedule(request("retry-held-1", null, NOW.plusSeconds(30),
                null, 3, 1, 1_000, 100, 25, 10_000, 500, 75));
        control.schedule(request("retry-held-2", null, NOW.plusSeconds(30),
                null, 3, 1, 1_000, 100, 25, 10_000, 500, 75));
        control.schedule(request("retry-capacity", null, NOW.plusSeconds(30),
                null, 3, 1, 1_000, 100, 25, 10_000, 500, 75));
        clock.set(NOW.plusSeconds(1));
        var heldOne = admission.admit(claim("retry-held-1", "worker-held-1"));
        var heldTwo = admission.admit(claim("retry-held-2", "worker-held-2"));
        assertThat(heldOne.disposition()).isEqualTo(PERMITTED);
        assertThat(heldTwo.disposition()).isEqualTo(PERMITTED);
        try {
            assertThat(admission.admit(
                    claim("retry-capacity", "worker-capacity")).disposition())
                    .isEqualTo(CAPACITY_FULL);
            var capacityRefused = schedules.current("tenant-ca", "retry-capacity");
            assertThat(capacityRefused.state())
                    .isEqualTo(RetryScheduleReceipt.State.SCHEDULED);
            assertThat(capacityRefused.attemptsUsed()).isEqualTo(1);
            assertThat(capacityRefused.tokensUsed()).isEqualTo(100);
            assertThat(capacityRefused.costUsedMicros()).isEqualTo(500);
        } finally {
            heldOne.permit().completeDependencyFailure(Duration.ofMillis(5),
                    new RetryableDependencyException("503"));
            heldTwo.permit().completeSuccess(Duration.ofMillis(5));
        }

        // The test breaker opens after one recorded failure.
        scheduleDue("retry-open");
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(admission.admit(claim("retry-open", "worker-open")).disposition())
                .isEqualTo(CIRCUIT_OPEN);
        var breakerRefused = schedules.current("tenant-ca", "retry-open");
        assertThat(breakerRefused.state())
                .isEqualTo(RetryScheduleReceipt.State.SCHEDULED);
        assertThat(breakerRefused.attemptsUsed()).isEqualTo(1);
        assertThat(breakerRefused.tokensUsed()).isEqualTo(100);
        assertThat(breakerRefused.costUsedMicros()).isEqualTo(500);
        assertThat(CircuitBreaker.class.getPackage().getImplementationVersion())
                .isEqualTo("2.4.0");
    }

    @Test
    // tag::hidden-retry-multiplication-test[]
    void camelAndTheJdkHttpGatewayProduceOnePermitAndOnePhysical503Request() throws Exception {
        scheduleDue("retry-http");
        var calls = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/model", exchange -> {
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            byte[] body = "unavailable".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            var first = producer.requestBody("direct:claim-due-retry",
                    claim("retry-http", "http-worker"), RetryAdmissionResult.class);
            var duplicate = producer.requestBody("direct:claim-due-retry",
                    claim("retry-http", "http-worker"), RetryAdmissionResult.class);
            assertThat(first.disposition()).isEqualTo(PERMITTED);
            assertThat(duplicate.disposition()).isEqualTo(NOT_DUE_OR_ALREADY_CLAIMED);

            var gateway = new SingleAttemptHttpGateway(Duration.ofSeconds(1));
            var response = gateway.post(URI.create("http://127.0.0.1:"
                    + server.getAddress().getPort() + "/model"), "{}", Duration.ofSeconds(1));
            assertThat(response.statusCode()).isEqualTo(503);
            first.permit().completeDependencyFailure(Duration.ofMillis(5),
                    new RetryableDependencyException("503"));
            assertThat(calls).hasValue(1);
        } finally {
            server.stop(0);
        }

        String openAiConfig = new ClassPathResource("application-openai.yml")
                .getContentAsString(StandardCharsets.UTF_8);
        assertThat(openAiConfig).contains("retry:\n      max-attempts: 0")
                .contains("openai:\n      max-retries: 0");
    }
    // end::hidden-retry-multiplication-test[]

    @Test
    void unexpectedClaimDefectEscapesTheCamelRouteWithoutRedelivery() {
        scheduleDue("retry-defective-claim");
        assertThatThrownBy(() -> producer.requestBody("direct:claim-due-retry",
                new RetryClaimCommand("tenant-ca", "retry-defective-claim", "worker",
                        Duration.ZERO)))
                .isInstanceOf(CamelExecutionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(schedules.current("tenant-ca", "retry-defective-claim").state())
                .isEqualTo(RetryScheduleReceipt.State.SCHEDULED);
    }

    @Test
    void unexpectedExecutionDefectIsPropagatedWithoutPoisoningBreakerMetrics() {
        scheduleDue("retry-defect-after-admission");
        var result = admission.admit(claim("retry-defect-after-admission", "defect-worker"));
        long failedBefore = circuitBreaker.getMetrics().getNumberOfFailedCalls();

        assertThatThrownBy(() -> result.permit().propagateUnexpected(
                new NullPointerException("adapter defect")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("adapter defect");

        assertThat(circuitBreaker.getMetrics().getNumberOfFailedCalls())
                .isEqualTo(failedBefore);
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(schedules.current("tenant-ca", "retry-defect-after-admission").state())
                .isEqualTo(CONSUMED);
    }

    private void scheduleDue(String scheduleId) {
        control.schedule(request(scheduleId, null, NOW.plusSeconds(30),
                null, 3, 1, 1_000, 100, 25, 10_000, 500, 75));
        clock.set(NOW.plusSeconds(1));
    }

    private RetryClaimCommand claim(String scheduleId, String worker) {
        return new RetryClaimCommand(
                "tenant-ca", scheduleId, worker, Duration.ofSeconds(5));
    }

    private RetryPolicyRequest request(
            String scheduleId, EffectReceipt effect, Instant deadline, Instant retryNotBefore,
            int maxAttempts, int attemptsUsed,
            long maxTokens, long tokensUsed, long nextTokens,
            long maxCost, long costUsed, long nextCost) {
        return request(scheduleId, effect, deadline, retryNotBefore, maxAttempts, attemptsUsed,
                maxTokens, tokensUsed, nextTokens, maxCost, costUsed, nextCost,
                retryableFailure(scheduleId));
    }

    private RetryPolicyRequest request(
            String scheduleId, EffectReceipt effect, Instant deadline, Instant retryNotBefore,
            int maxAttempts, int attemptsUsed,
            long maxTokens, long tokensUsed, long nextTokens,
            long maxCost, long costUsed, long nextCost, FailureDecision failure) {
        return new RetryPolicyRequest(
                "tenant-ca", scheduleId, "model-assessment", failure, effect,
                deadline, retryNotBefore, Duration.ofSeconds(1), Duration.ofSeconds(8),
                Duration.ofSeconds(10), Duration.ofSeconds(2), Duration.ofSeconds(2),
                maxAttempts, attemptsUsed,
                maxTokens, tokensUsed, nextTokens, maxCost, costUsed, nextCost);
    }

    private RetryPolicyRequest copyWithDeadline(RetryPolicyRequest request, Instant deadline) {
        return new RetryPolicyRequest(
                request.tenantId(), request.scheduleId(), request.operationKey(),
                request.failure(), request.effect(), deadline, request.trustedRetryNotBefore(),
                request.baseBackoff(), request.maximumBackoff(),
                request.configuredAttemptTimeout(), request.settlementReserve(),
                request.minimumUsefulAttemptTimeout(),
                request.maxAttempts(), request.attemptsUsed(), request.maxTokens(),
                request.tokensUsed(), request.nextAttemptTokens(), request.maxCostMicros(),
                request.costUsedMicros(), request.nextAttemptCostMicros());
    }

    private FailureDecision retryableFailure(String identity) {
        return new FailureDecision(
                "tenant-ca", "run-14", "obs-" + identity, "orderdesk-failure-v1",
                ClassificationObservation.Stage.CAPABILITY_GATEWAY, NOW,
                ClassificationObservation.Kind.DEPENDENCY_UNAVAILABLE,
                FailureDecision.Disposition.RETRY_ELIGIBLE,
                FailureDecision.RunConsequence.KEEP_RUNNING,
                FailureDecision.RetryEligibility.GOVERNED_RETRY_MAY_SUCCEED,
                FailureDecision.OperatorAction.OBSERVE_DEPENDENCY,
                "DEPENDENCY_503", "evidence-14", null, null);
    }

    private FailureDecision deniedFailure() {
        return new FailureDecision(
                "tenant-ca", "run-14", "obs-denied", "orderdesk-failure-v1",
                ClassificationObservation.Stage.CAPABILITY_GATEWAY, NOW,
                ClassificationObservation.Kind.POLICY_DENIED,
                FailureDecision.Disposition.DENIED,
                FailureDecision.RunConsequence.KEEP_RUNNING,
                FailureDecision.RetryEligibility.NEW_AUTHORIZATION_OR_POLICY_REQUIRED,
                FailureDecision.OperatorAction.REVIEW_IDENTITY_OR_POLICY,
                "POLICY_DENIED", "evidence-denied", null, null);
    }

    private EffectReceipt effect(EffectReceipt.State state, Instant expiresAt) {
        return effect(state, "effect-key-14", expiresAt);
    }

    private EffectReceipt effect(
            EffectReceipt.State state, String idempotencyKey, Instant expiresAt) {
        return new EffectReceipt(
                EffectReceipt.Disposition.OUTCOME_RECORDED, "tenant-ca", "effect-14",
                state, 2, 1, idempotencyKey, expiresAt, null);
    }

    @TestConfiguration
    static class TestBeans {
        @Bean
        @Primary
        AdjustableClock retryClock() {
            return new AdjustableClock(NOW);
        }

        @Bean
        @Primary
        EqualJitterBackoff deterministicJitter() {
            return new EqualJitterBackoff(() -> 0.0);
        }

        @Bean
        @Primary
        RetryCapacityGate boundedRetryCapacity() {
            return new SemaphoreRetryCapacityGate(2);
        }

        @Bean
        @Primary
        CircuitBreaker testRetryCircuitBreaker() {
            var config = CircuitBreakerConfig.custom()
                    .slidingWindowSize(1)
                    .minimumNumberOfCalls(1)
                    .failureRateThreshold(50.0f)
                    .waitDurationInOpenState(Duration.ofMinutes(1))
                    .build();
            return CircuitBreaker.of("chapter-14-test", config);
        }
    }

    static final class AdjustableClock extends Clock {
        private final AtomicReference<Instant> now;

        AdjustableClock(Instant initial) {
            now = new AtomicReference<>(initial);
        }

        void set(Instant value) {
            now.set(value);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
