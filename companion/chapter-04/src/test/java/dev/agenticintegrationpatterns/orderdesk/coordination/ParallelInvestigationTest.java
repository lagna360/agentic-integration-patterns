package dev.agenticintegrationpatterns.orderdesk.coordination;

import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
import dev.agenticintegrationpatterns.orderdesk.context.ContextResolutionRequest;
import dev.agenticintegrationpatterns.orderdesk.context.ContextResolutionService;
import dev.agenticintegrationpatterns.orderdesk.context.ResolvedInvestigationContext;
import dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation;
import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import dev.agenticintegrationpatterns.orderdesk.work.TrustedAdmissionContext;
import org.apache.camel.CamelExecutionException;
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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationBranch.INVENTORY_RECHECK;
import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationBranch.ORDER_HISTORY;
import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationReply.Status.SUCCEEDED;
import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationReply.Status.UNAVAILABLE;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceReducer.MergeDisposition.*;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.Completion.*;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.CompletionTrigger.*;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.Reason.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(ParallelInvestigationTest.TestBeans.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false",
        "orderdesk.parallel.timeout-ms=250"
})
class ParallelInvestigationTest {
    private static final Instant NOW = Instant.parse("2026-08-24T06:14:00Z");

    @Autowired ObjectMapper mapper;
    @Autowired ContextResolutionService contextResolver;
    @Autowired ParallelInvestigationPlanProvider plans;
    @Autowired ParallelEvidenceReducer reducer;
    @Autowired ControllableParallelInvestigator investigator;
    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;
    @Autowired JdbcTemplate jdbc;

    private InvestigateOrderException command;
    private int runSequence;

    @BeforeEach
    void reset() throws Exception {
        jdbc.update("delete from context_snapshot_item");
        jdbc.update("delete from context_snapshot");
        jdbc.update("delete from artifact_view");
        jdbc.update("delete from artifact_content");
        investigator.reset();
        runSequence = 0;
        command = mapper.readValue(getClass().getResourceAsStream(
                "/fixtures/investigate-order-exception-v1.json"),
                InvestigateOrderException.class);
        drain("seda:parallel-evidence-ready");
        drain("seda:parallel-evidence-review");
    }

    @Test
    // tag::parallel-route-test[]
    void fixedRecipientsRunConcurrentlyAndProduceOneCompleteEvidenceSet() {
        investigator.requireConcurrentStart();
        var request = new ParallelInvestigationRequest(
                resolved(Set.of("read-inventory", "read-order")));

        var exchange = producer.request("direct:parallel-investigation", sent -> {
            sent.getMessage().setBody(request);
            sent.getMessage().setHeaders(Map.of(
                    "CamelRecipientListEndpoint", "direct:attacker",
                    "parallelRecipients", "http://attacker.example",
                    "tenantId", "tenant-attacker"));
        });

        var result = exchange.getMessage().getBody(ParallelEvidenceSet.class);
        assertThat(result.completion()).isEqualTo(COMPLETE);
        assertThat(result.disposition()).isEqualTo(EVIDENCE_READY);
        assertThat(result.reason()).isEqualTo(ALL_EXPECTED_EVIDENCE);
        assertThat(result.replies()).extracting(InvestigationReply::branch)
                .containsExactly(INVENTORY_RECHECK, ORDER_HISTORY);
        assertThat(investigator.calls(INVENTORY_RECHECK)).isEqualTo(1);
        assertThat(investigator.calls(ORDER_HISTORY)).isEqualTo(1);
        assertThat(exchange.getMessage().getHeaders())
                .doesNotContainKeys("CamelRecipientListEndpoint", "parallelRecipients", "tenantId");
    }
    // end::parallel-route-test[]

    @Test
    void optionalTimeoutIsExplicitAndLateTaskCannotMutateReturnedSnapshot() throws Exception {
        investigator.delay(ORDER_HISTORY, 600);
        var request = new ParallelInvestigationRequest(
                resolved(Set.of("read-inventory", "read-order")));

        var result = producer.requestBody(
                "direct:parallel-investigation", request, ParallelEvidenceSet.class);

        assertThat(result.completion()).isEqualTo(PARTIAL);
        assertThat(result.disposition()).isEqualTo(EVIDENCE_READY);
        assertThat(result.reason()).isEqualTo(OPTIONAL_EVIDENCE_MISSING);
        assertThat(result.completionTrigger()).isEqualTo(TIMEOUT);
        assertThat(result.missingBranches()).containsExactly(ORDER_HISTORY);

        Thread.sleep(700);
        assertThat(result.replies()).extracting(InvestigationReply::branch)
                .containsExactly(INVENTORY_RECHECK);
    }

    @Test
    void requiredTimeoutRoutesThePartialSetToManualReview() throws Exception {
        investigator.delay(INVENTORY_RECHECK, 600);
        var request = new ParallelInvestigationRequest(
                resolved(Set.of("read-inventory", "read-order")));

        var result = producer.requestBody(
                "direct:parallel-investigation", request, ParallelEvidenceSet.class);

        assertThat(result.completion()).isEqualTo(PARTIAL);
        assertThat(result.disposition()).isEqualTo(MANUAL_REVIEW);
        assertThat(result.reason()).isEqualTo(REQUIRED_EVIDENCE_MISSING);
        assertThat(result.missingBranches()).containsExactly(INVENTORY_RECHECK);

        // Camel's timeout returns control; it does not promise task cancellation.
        // Let this fixture task leave the bounded pool before another test uses it.
        Thread.sleep(700);
    }

    @Test
    void unavailableOptionalReplyIsRetainedAsPartialRatherThanDisappearing() {
        investigator.unavailable(ORDER_HISTORY);
        var request = new ParallelInvestigationRequest(
                resolved(Set.of("read-inventory", "read-order")));

        var result = producer.requestBody(
                "direct:parallel-investigation", request, ParallelEvidenceSet.class);

        assertThat(result.completion()).isEqualTo(PARTIAL);
        assertThat(result.disposition()).isEqualTo(EVIDENCE_READY);
        assertThat(result.completionTrigger()).isEqualTo(ALL_REPLIES);
        assertThat(result.unavailableBranches()).containsExactly(ORDER_HISTORY);
        assertThat(result.replies()).hasSize(2);
    }

    @Test
    void effectiveGrantsDetermineTheServerOwnedRecipientSet() {
        var request = new ParallelInvestigationRequest(resolved(Set.of("read-inventory")));

        var result = producer.requestBody(
                "direct:parallel-investigation", request, ParallelEvidenceSet.class);

        assertThat(result.completion()).isEqualTo(COMPLETE);
        assertThat(result.replies()).extracting(InvestigationReply::branch)
                .containsExactly(INVENTORY_RECHECK);
        assertThat(investigator.calls(ORDER_HISTORY)).isZero();
    }

    @Test
    // tag::reducer-failure-test[]
    void duplicateConflictAndLateRepliesHaveDistinctOutcomes() {
        var plan = plans.plan(new ParallelInvestigationRequest(
                resolved(Set.of("read-inventory", "read-order"))));
        var inventory = reply(plan, INVENTORY_RECHECK,
                "inventory.availableUnits:sku", "0", "inventory-ledger");
        var changedInventory = reply(plan, INVENTORY_RECHECK,
                "inventory.availableUnits:sku", "7", "inventory-ledger");

        var first = reducer.merge(reducer.start(plan), inventory, NOW);
        var duplicate = reducer.merge(first.accumulator(), inventory, NOW);
        var conflict = reducer.merge(duplicate.accumulator(), changedInventory, NOW);

        assertThat(first.disposition()).isEqualTo(ADDED);
        assertThat(duplicate.disposition()).isEqualTo(DUPLICATE);
        assertThat(duplicate.accumulator().duplicateCount()).isEqualTo(1);
        assertThat(conflict.disposition()).isEqualTo(CONFLICT);
        assertThat(reducer.close(conflict.accumulator(), NOW, ALL_REPLIES))
                .satisfies(result -> {
                    assertThat(result.completion()).isEqualTo(CONFLICTED);
                    assertThat(result.disposition()).isEqualTo(MANUAL_REVIEW);
                    assertThat(result.reason()).isEqualTo(EVIDENCE_CONFLICT);
                });

        var closed = new ParallelEvidenceAccumulator(
                plan, first.accumulator().replies(), first.accumulator().conflicts(),
                first.accumulator().duplicateCount(), NOW, TIMEOUT);
        var order = reply(plan, ORDER_HISTORY,
                "order.fulfillmentState:order-100045", "BACKORDERED", "order-service");
        var late = reducer.merge(closed, order, NOW);
        assertThat(late.disposition()).isEqualTo(LATE);
        assertThat(late.accumulator().replies()).containsOnlyKeys(INVENTORY_RECHECK);
    }
    // end::reducer-failure-test[]

    @Test
    void contradictoryCrossBranchEvidenceIsPreservedAsConflict() {
        var plan = plans.plan(new ParallelInvestigationRequest(
                resolved(Set.of("read-inventory", "read-order"))));
        var inventory = reply(plan, INVENTORY_RECHECK,
                "allocation.state:order-100045", "UNALLOCATED", "inventory-ledger");
        var order = reply(plan, ORDER_HISTORY,
                "allocation.state:order-100045", "ALLOCATED", "order-service");

        var first = reducer.merge(reducer.start(plan), inventory, NOW);
        var second = reducer.merge(first.accumulator(), order, NOW);
        var result = reducer.close(second.accumulator(), NOW, ALL_REPLIES);

        assertThat(second.disposition()).isEqualTo(CONFLICT);
        assertThat(result.conflicts()).singleElement().satisfies(conflict -> {
            assertThat(conflict.firstBranch()).isEqualTo(INVENTORY_RECHECK);
            assertThat(conflict.secondBranch()).isEqualTo(ORDER_HISTORY);
            assertThat(conflict.firstValueSha256())
                    .isNotEqualTo(conflict.secondValueSha256());
        });
        assertThat(result.completion()).isEqualTo(CONFLICTED);
    }

    @Test
    void foreignTenantAndInvalidDigestAreRejectedBeforeAggregation() {
        var plan = plans.plan(new ParallelInvestigationRequest(
                resolved(Set.of("read-inventory", "read-order"))));
        var valid = reply(plan, INVENTORY_RECHECK,
                "inventory.availableUnits:sku", "0", "inventory-ledger");
        var foreign = new InvestigationReply(
                valid.replyId(), valid.scatterId(), valid.runId(), "tenant-attacker",
                valid.branch(), valid.status(), valid.finding(), valid.completedAt());
        var invalidDigest = new InvestigationReply(
                valid.replyId(), valid.scatterId(), valid.runId(), valid.tenantId(),
                valid.branch(), valid.status(), new EvidenceFinding(
                        valid.finding().evidenceKey(), valid.finding().canonicalValue(),
                        "0".repeat(64), valid.finding().sourceSystem(),
                        valid.finding().sourceVersion(), valid.finding().observedAt()),
                valid.completedAt());

        assertThatThrownBy(() -> reducer.merge(reducer.start(plan), foreign, NOW))
                .isInstanceOf(InvalidInvestigationReplyException.class);
        assertThatThrownBy(() -> reducer.merge(reducer.start(plan), invalidDigest, NOW))
                .isInstanceOf(InvalidInvestigationReplyException.class);

        var closed = new ParallelEvidenceAccumulator(
                plan, Map.of(), List.of(), 0, NOW, TIMEOUT);
        assertThatThrownBy(() -> reducer.merge(closed, foreign, NOW))
                .isInstanceOf(InvalidInvestigationReplyException.class);
    }

    @Test
    void unexpectedBranchProgrammingFaultRemainsARouteFailure() {
        investigator.failUnexpectedly(INVENTORY_RECHECK);
        var request = new ParallelInvestigationRequest(
                resolved(Set.of("read-inventory", "read-order")));

        assertThatThrownBy(() -> producer.requestBody(
                "direct:parallel-investigation", request, ParallelEvidenceSet.class))
                .isInstanceOf(CamelExecutionException.class);
        assertThat(consumer.receiveBodyNoWait("seda:parallel-evidence-ready")).isNull();
        assertThat(consumer.receiveBodyNoWait("seda:parallel-evidence-review")).isNull();
    }

    private ResolvedInvestigationContext resolved(Set<String> grants) {
        String runId = "run-parallel-" + (++runSequence);
        var trusted = new TrustedAdmissionContext(
                "tenant-ca", "workload:order-exception-case-manager",
                Set.of("principal:order-ops-ca"), NOW);
        var admitted = new AdmittedInvestigation(
                command, trusted, grants, command.limits(),
                AdmittedInvestigation.ReplyContract.ORDER_EXCEPTION_CASE_RESULTS_V1);
        return contextResolver.resolve(new ContextResolutionRequest(runId, admitted));
    }

    private InvestigationReply reply(
            ParallelInvestigationPlan plan,
            InvestigationBranch branch,
            String key,
            String value,
            String source) {
        var finding = new EvidenceFinding(
                key, value, ParallelEvidenceDigests.sha256(value), source, "v1", NOW);
        return new InvestigationReply(
                "reply-" + branch.name().toLowerCase(), plan.scatterId(), plan.runId(),
                plan.tenantId(), branch, SUCCEEDED, finding, NOW);
    }

    private void drain(String endpoint) {
        while (consumer.receiveBodyNoWait(endpoint) != null) {
            // drain prior route output
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestBeans {
        @Bean
        @Primary
        Clock chapterTenClock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        @Primary
        ControllableParallelInvestigator controllableParallelInvestigator(Clock clock) {
            return new ControllableParallelInvestigator(clock);
        }
    }

    static final class ControllableParallelInvestigator implements ParallelInvestigator {
        private final Clock clock;
        private final EnumMap<InvestigationBranch, AtomicInteger> calls =
                new EnumMap<>(InvestigationBranch.class);
        private final EnumMap<InvestigationBranch, Long> delays =
                new EnumMap<>(InvestigationBranch.class);
        private final Set<InvestigationBranch> unavailable =
                java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final Set<InvestigationBranch> failures =
                java.util.concurrent.ConcurrentHashMap.newKeySet();
        private volatile CyclicBarrier startBarrier;

        ControllableParallelInvestigator(Clock clock) {
            this.clock = clock;
            for (var branch : InvestigationBranch.values()) {
                calls.put(branch, new AtomicInteger());
            }
        }

        @Override
        public InvestigationReply investigate(
                InvestigationBranch branch,
                ParallelBranchRequest request) {
            calls.get(branch).incrementAndGet();
            awaitBarrier();
            long delay = delays.getOrDefault(branch, 0L);
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Fixture branch interrupted", interrupted);
                }
            }
            if (failures.contains(branch)) {
                throw new NullPointerException("fixture branch programming fault");
            }
            var now = clock.instant();
            if (unavailable.contains(branch)) {
                return new InvestigationReply(
                        "reply-unavailable-" + branch.name().toLowerCase(),
                        request.plan().scatterId(), request.plan().runId(),
                        request.plan().tenantId(), branch, UNAVAILABLE, null, now);
            }
            String value = branch == INVENTORY_RECHECK ? "0" : "BACKORDERED";
            String key = branch == INVENTORY_RECHECK
                    ? "inventory.availableUnits:yyz-01:camera-battery-x2"
                    : "order.fulfillmentState:order-100045";
            String source = branch == INVENTORY_RECHECK ? "inventory-ledger" : "order-service";
            var finding = new EvidenceFinding(
                    key, value, ParallelEvidenceDigests.sha256(value), source, "fixture-v1", now);
            return new InvestigationReply(
                    "reply-" + branch.name().toLowerCase(), request.plan().scatterId(),
                    request.plan().runId(), request.plan().tenantId(), branch,
                    SUCCEEDED, finding, now);
        }

        void reset() {
            calls.values().forEach(value -> value.set(0));
            delays.clear();
            unavailable.clear();
            failures.clear();
            startBarrier = null;
        }

        void requireConcurrentStart() {
            startBarrier = new CyclicBarrier(2);
        }

        void delay(InvestigationBranch branch, long milliseconds) {
            delays.put(branch, milliseconds);
        }

        void unavailable(InvestigationBranch branch) {
            unavailable.add(branch);
        }

        void failUnexpectedly(InvestigationBranch branch) {
            failures.add(branch);
        }

        int calls(InvestigationBranch branch) {
            return calls.get(branch).get();
        }

        private void awaitBarrier() {
            CyclicBarrier barrier = startBarrier;
            if (barrier == null) {
                return;
            }
            try {
                barrier.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Fixture barrier interrupted", interrupted);
            } catch (BrokenBarrierException | TimeoutException failure) {
                throw new IllegalStateException("Branches did not start concurrently", failure);
            }
        }
    }
}
