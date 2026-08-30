package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
import dev.agenticintegrationpatterns.orderdesk.work.InvalidWorkEnvelopeException;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import({FixedKafkaClockTestConfiguration.class,
        KafkaIngressFailureContractTest.FailOnceHookConfiguration.class})
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class KafkaIngressFailureContractTest {
    private static final String DIRECT_URI = "direct:kafka-ingress-contract-test";
    private static final String TOPIC = "orderdesk.investigation.commands.v1";
    private static final String VALID_KEY =
            "v1|tenant-ca|case-d5a30e20-f10b-38ca-9198-4834746bd37b";

    @Autowired CamelContext camelContext;
    @Autowired ProducerTemplate producer;
    @Autowired KafkaIngressPipeline pipeline;
    @Autowired KafkaFailureDispositionProcessor failureDisposition;
    @Autowired ManualKafkaCommitProcessor manualCommit;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired FailOnceAfterInboxHook failOnceHook;

    private String validPayload;

    @BeforeAll
    void addProductionEquivalentPipeline() throws Exception {
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                onException(InvalidWorkEnvelopeException.class, InvalidKafkaRecordException.class)
                        .handled(true)
                        .process(failureDisposition)
                        .process(manualCommit);
                from(DIRECT_URI)
                        .routeId("kafka-ingress-contract-test")
                        .process(pipeline)
                        .process(manualCommit);
            }
        });
    }

    @BeforeEach
    void reset() throws Exception {
        jdbc.update("delete from ingress_quarantine");
        jdbc.update("delete from admitted_work");
        jdbc.update("delete from command_inbox");
        failOnceHook.reset();
        validPayload = new String(getClass().getResourceAsStream(
                "/fixtures/investigate-order-exception-v1.json").readAllBytes(),
                StandardCharsets.UTF_8);
    }

    @Test
    // tag::forced-redelivery-test[]
    void crashAfterInboxCommitLeavesOffsetUncommittedThenRedeliveryAcknowledgesDuplicate() {
        var commits = new CountingCommit();
        failOnceHook.failNext();

        assertThatThrownBy(() -> send(validPayload, VALID_KEY, 41, commits))
                .hasRootCauseMessage("forced crash after inbox commit");
        assertThat(commits.count()).isZero();
        assertThat(rowCount("admitted_work")).isEqualTo(1);

        send(validPayload, VALID_KEY, 41, commits);

        assertThat(commits.count()).isEqualTo(1);
        assertThat(rowCount("admitted_work")).isEqualTo(1);
        assertThat(rowCount("command_inbox")).isEqualTo(1);
    }
    // end::forced-redelivery-test[]

    @Test
    void malformedCommandIsDurablyQuarantinedBeforeAcknowledgement() {
        assertDurableRejection("not-json", VALID_KEY, 50, "WORK_ENVELOPE_MALFORMED");
    }

    @Test
    void keyMismatchIsDurablyQuarantinedBeforeAcknowledgement() {
        assertDurableRejection(validPayload, "v1|tenant-us|wrong-case", 51,
                "TRANSPORT_MAPPING_REJECTED");
    }

    @Test
    void expiredCommandIsDurablyQuarantinedBeforeAcknowledgement() throws Exception {
        var expired = mapper.readTree(validPayload);
        ((tools.jackson.databind.node.ObjectNode) expired)
                .put("deadlineAt", "2026-08-24T06:13:59Z");
        assertDurableRejection(mapper.writeValueAsString(expired), VALID_KEY, 52,
                "WORK_ENVELOPE_EXPIRED");
    }

    private void assertDurableRejection(
            String body, String key, long offset, String expectedReason) {
        var commits = new CountingCommit();
        send(body, key, offset, commits);

        assertThat(commits.count()).isEqualTo(1);
        assertThat(rowCount("ingress_quarantine")).isEqualTo(1);
        assertThat(rowCount("admitted_work")).isZero();
        assertThat(jdbc.queryForObject(
                "select reason from ingress_quarantine", String.class))
                .isEqualTo(expectedReason);
        assertThat(jdbc.queryForObject(
                "select length(received_fingerprint) from ingress_quarantine", Integer.class))
                .isEqualTo(64);
    }

    private void send(String body, String key, long offset, KafkaManualCommit commit) {
        producer.requestBodyAndHeaders(DIRECT_URI, body, Map.of(
                KafkaConstants.TOPIC, TOPIC,
                KafkaConstants.PARTITION, 0,
                KafkaConstants.OFFSET, offset,
                KafkaConstants.KEY, key,
                KafkaConstants.MANUAL_COMMIT, commit));
    }

    private int rowCount(String table) {
        return jdbc.queryForObject("select count(*) from " + table, Integer.class);
    }

    private static final class CountingCommit implements KafkaManualCommit {
        private final AtomicInteger count = new AtomicInteger();

        @Override
        public void commit() {
            count.incrementAndGet();
        }

        int count() {
            return count.get();
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class FailOnceHookConfiguration {
        @Bean
        @Primary
        FailOnceAfterInboxHook failOnceAfterInboxHook() {
            return new FailOnceAfterInboxHook();
        }
    }

    static final class FailOnceAfterInboxHook implements AfterInboxHook {
        private final AtomicBoolean failNext = new AtomicBoolean();

        void failNext() {
            failNext.set(true);
        }

        void reset() {
            failNext.set(false);
        }

        @Override
        public void afterInbox(org.apache.camel.Exchange exchange) {
            if (failNext.compareAndSet(true, false)) {
                throw new IllegalStateException("forced crash after inbox commit");
            }
        }
    }
}
