package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.OrderExceptionApplication;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@Import(FixedKafkaClockTestConfiguration.class)
@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=true"
})
class KafkaIngressIT {
    private static final String TOPIC = "orderdesk.investigation.commands.v1";
    private static final String ADMISSION_GROUP = "orderdesk-investigation-admission-v1";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse(System.getProperty("kafka.test.image", "apache/kafka:4.3.1")));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("orderdesk.kafka.brokers", KAFKA::getBootstrapServers);
    }

    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void reset() throws Exception {
        try (var admin = AdminClient.create(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers()))) {
            try {
                admin.createTopics(java.util.List.of(new NewTopic(TOPIC, 2, (short) 1)))
                        .all().get(10, TimeUnit.SECONDS);
            } catch (Exception alreadyExists) {
                // The class uses one shared broker; an existing topic is expected after the first test.
            }
        }
        jdbc.update("delete from ingress_quarantine");
        jdbc.update("delete from admitted_work");
        jdbc.update("delete from command_inbox");
        awaitConsumerAssignment(Duration.ofSeconds(30));
    }

    @Test
    void pinnedBrokerDeliversTheChapterFiveCommandToCamelAndTheSqlInbox() throws Exception {
        String payload = new String(getClass().getResourceAsStream(
                "/fixtures/investigate-order-exception-v1.json").readAllBytes(), StandardCharsets.UTF_8);
        String key = "v1|tenant-ca|case-d5a30e20-f10b-38ca-9198-4834746bd37b";
        RecordMetadata sent = send(key, payload);

        awaitCount("admitted_work", 1, Duration.ofSeconds(20));
        awaitCommittedOffset(sent.offset() + 1, Duration.ofSeconds(20));
        assertThat(AppInfoParser.getVersion()).isEqualTo("4.3.1");
        assertThat(jdbc.queryForObject("select count(*) from command_inbox", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select first_topic from command_inbox", String.class))
                .isEqualTo(TOPIC);
        assertThat(jdbc.queryForObject("select first_partition from command_inbox", Integer.class))
                .isBetween(0, 1);
    }

    @Test
    void pinnedBrokerDurablyQuarantinesMalformedValueBeforeAdvancing() throws Exception {
        RecordMetadata sent = send(
                "v1|tenant-ca|case-d5a30e20-f10b-38ca-9198-4834746bd37b", "not-json");

        awaitCount("ingress_quarantine", 1, Duration.ofSeconds(20));
        awaitCommittedOffset(sent.offset() + 1, Duration.ofSeconds(20));
        assertThat(jdbc.queryForObject("select count(*) from admitted_work", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject(
                "select reason from ingress_quarantine", String.class))
                .isEqualTo("WORK_ENVELOPE_MALFORMED");
        assertThat(jdbc.queryForObject(
                "select source_offset from ingress_quarantine", Long.class))
                .isEqualTo(sent.offset());
    }

    private RecordMetadata send(String key, String payload) throws Exception {
        var properties = Map.<String, Object>of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName(),
                "acks", "all");
        try (var producer = new KafkaProducer<String, String>(properties)) {
            var sent = producer.send(new ProducerRecord<>(TOPIC, key, payload))
                    .get(10, TimeUnit.SECONDS);
            producer.flush();
            return sent;
        }
    }

    private void awaitConsumerAssignment(Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        try (var admin = AdminClient.create(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers()))) {
            while (System.nanoTime() < deadline) {
                try {
                    var groups = admin.describeConsumerGroups(java.util.List.of(ADMISSION_GROUP))
                            .all().get(5, TimeUnit.SECONDS);
                    var group = groups.get(ADMISSION_GROUP);
                    boolean assigned = group != null && group.members().stream()
                            .flatMap(member -> member.assignment().topicPartitions().stream())
                            .anyMatch(partition -> partition.topic().equals(TOPIC));
                    if (assigned) {
                        return;
                    }
                } catch (Exception groupNotReady) {
                    // Topic discovery and the initial group join are asynchronous.
                }
                Thread.sleep(100);
            }
        }
        throw new AssertionError("Kafka consumer was not assigned before the test published a record");
    }

    private void awaitCommittedOffset(long minimumOffset, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        try (var admin = AdminClient.create(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers()))) {
            while (System.nanoTime() < deadline) {
                var offsets = admin.listConsumerGroupOffsets(
                                "orderdesk-investigation-admission-v1")
                        .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                boolean committed = offsets.entrySet().stream()
                        .anyMatch(entry -> entry.getKey().topic().equals(TOPIC)
                                && entry.getValue().offset() >= minimumOffset);
                if (committed) {
                    return;
                }
                Thread.sleep(100);
            }
        }
        throw new AssertionError("Kafka offset was not committed after durable inbox admission");
    }

    private void awaitCount(String table, int expected, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (jdbc.queryForObject("select count(*) from " + table, Integer.class) == expected) {
                return;
            }
            Thread.sleep(100);
        }
        assertThat(jdbc.queryForObject("select count(*) from " + table, Integer.class))
                .isEqualTo(expected);
    }
}
