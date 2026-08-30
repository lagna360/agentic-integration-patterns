package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SpecificationVersion;
import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
import dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation;
import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;
import dev.agenticintegrationpatterns.orderdesk.work.TrustedAdmissionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static dev.agenticintegrationpatterns.orderdesk.transport.kafka.IngressReceipt.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation.ReplyContract.ORDER_EXCEPTION_CASE_RESULTS_V1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none",
        "orderdesk.kafka.enabled=false"
})
class KafkaTransportInboxTest {
    private static final String PRODUCER = "workload:order-exception-case-manager";

    @Autowired ObjectMapper mapper;
    @Autowired JdbcCommandInbox inbox;
    @Autowired JdbcTemplate jdbc;

    private InvestigateOrderException command;

    @BeforeEach
    void reset() throws Exception {
        jdbc.update("delete from ingress_quarantine");
        jdbc.update("delete from admitted_work");
        jdbc.update("delete from command_inbox");
        command = mapper.readValue(getClass().getResourceAsStream(
                "/fixtures/investigate-order-exception-v1.json"), InvestigateOrderException.class);
    }

    @Test
    void tenantScopedCaseKeyIsRoutingDataThatMustMatchTheBody() {
        assertThat(TenantCaseKey.from(command)).isEqualTo(
                "v1|tenant-ca|case-d5a30e20-f10b-38ca-9198-4834746bd37b");
        assertThatThrownBy(() -> TenantCaseKey.requireMatch(
                "v1|tenant-us|case-d5a30e20-f10b-38ca-9198-4834746bd37b", command))
                .isInstanceOf(InvalidKafkaRecordException.class);
    }

    @Test
    // tag::forced-redelivery-test[]
    void redeliveryAfterLocalCommitCreatesOneRunAndReturnsTheRecordedDecision() {
        var admitted = admitted(command);
        String fingerprint = PayloadFingerprint.canonical(mapper, command);

        var first = inbox.admit(admitted, PRODUCER, fingerprint, position(41));
        // Simulates a crash before the Kafka offset commit: the same record arrives again.
        var redelivery = inbox.admit(admitted, PRODUCER, fingerprint, position(41));

        assertThat(first.disposition()).isEqualTo(ACCEPTED);
        assertThat(redelivery.disposition()).isEqualTo(DUPLICATE_SAME);
        assertThat(redelivery.runId()).isEqualTo(first.runId());
        assertThat(jdbc.queryForObject("select count(*) from admitted_work", Integer.class))
                .isEqualTo(1);
    }
    // end::forced-redelivery-test[]

    @Test
    void sameScopedIdentityWithDifferentContentIsQuarantinedNotReinterpreted() {
        var admitted = admitted(command);
        inbox.admit(admitted, PRODUCER, PayloadFingerprint.canonical(mapper, command), position(41));
        var changed = new InvestigateOrderException(
                command.schemaVersion(), command.commandId(), command.type(), command.caseId(),
                command.correlationId(), command.causedBy(), command.issuedAt(), command.deadlineAt(),
                command.tenantId(), command.principalRef(), command.requestingWorkloadRef(),
                command.objective(), new InvestigateOrderException.ExecutionLimits(3, 8, 2_000),
                command.requestedCapabilities(), command.evidence(), command.configuration(),
                command.replyContract());

        var collision = inbox.admit(admitted(changed), PRODUCER,
                PayloadFingerprint.canonical(mapper, changed), position(42));

        assertThat(collision.disposition()).isEqualTo(DUPLICATE_CONFLICT);
        assertThat(jdbc.queryForObject("select count(*) from admitted_work", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from ingress_quarantine", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void inboxAndAdmittedWorkRollbackTogetherWhenSecondInsertFails() {
        String runId = deterministicRunId(command);
        jdbc.update("""
                insert into admitted_work
                (tenant_id, authenticated_producer_ref, command_id, run_id, case_id, correlation_id)
                values ('other-tenant', 'other-workload', 'other-command', ?, 'other-case', 'other-correlation')
                """, runId);

        assertThatThrownBy(() -> inbox.admit(admitted(command), PRODUCER,
                PayloadFingerprint.canonical(mapper, command), position(41)))
                .isInstanceOf(RuntimeException.class);
        assertThat(jdbc.queryForObject("select count(*) from command_inbox", Integer.class))
                .isZero();
    }

    @Test
    void schemaAndCanonicalFixtureValidateUnderDraft202012() throws Exception {
        var schemaNode = mapper.readTree(getClass().getResourceAsStream(
                "/contracts/investigate-order-exception-command-v1.schema.json"));
        var fixture = mapper.readTree(getClass().getResourceAsStream(
                "/fixtures/investigate-order-exception-v1.json"));
        var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
        var metaSchema = registry.getSchema(SchemaLocation.of(
                SpecificationVersion.DRAFT_2020_12.getDialectId()));
        var commandSchema = registry.getSchema(schemaNode);

        assertThat(metaSchema.validate(schemaNode)).isEmpty();
        assertThat(commandSchema.validate(fixture)).isEmpty();

        var missingRequired = fixture.deepCopy();
        ((tools.jackson.databind.node.ObjectNode) missingRequired).remove("commandId");
        assertThat(commandSchema.validate(missingRequired)).isNotEmpty();

        var unknownProperty = fixture.deepCopy();
        ((tools.jackson.databind.node.ObjectNode) unknownProperty).put("surprise", true);
        assertThat(commandSchema.validate(unknownProperty)).isNotEmpty();
    }

    private AdmittedInvestigation admitted(InvestigateOrderException value) {
        return new AdmittedInvestigation(value,
                new TrustedAdmissionContext("tenant-ca", PRODUCER,
                        Set.of("principal:order-ops-ca"), Instant.parse("2026-08-24T06:14:00Z")),
                Set.copyOf(value.requestedCapabilities()), value.limits(),
                ORDER_EXCEPTION_CASE_RESULTS_V1);
    }

    private KafkaTransportPosition position(long offset) {
        return new KafkaTransportPosition("orderdesk.investigation.commands.v1", 0, offset,
                TenantCaseKey.from(command));
    }

    private String deterministicRunId(InvestigateOrderException value) {
        return "run-" + UUID.nameUUIDFromBytes((value.tenantId() + "|" + PRODUCER + "|"
                + value.commandId()).getBytes(StandardCharsets.UTF_8));
    }
}
