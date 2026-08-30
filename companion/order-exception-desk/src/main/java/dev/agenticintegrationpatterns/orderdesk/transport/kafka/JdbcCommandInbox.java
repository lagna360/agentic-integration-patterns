package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static dev.agenticintegrationpatterns.orderdesk.transport.kafka.IngressReceipt.Disposition.*;

@Component
public class JdbcCommandInbox {
    private final JdbcTemplate jdbc;

    public JdbcCommandInbox(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // tag::inbox-transaction[]
    @Transactional
    public IngressReceipt admit(
            AdmittedInvestigation admitted,
            String authenticatedProducerRef,
            String fingerprint,
            KafkaTransportPosition position) {
        var command = admitted.command();
        var existing = jdbc.query(
                """
                select payload_fingerprint, run_id from command_inbox
                 where tenant_id=? and authenticated_producer_ref=? and command_id=?
                """,
                (rs, row) -> new Existing(rs.getString(1), rs.getString(2)),
                command.tenantId(), authenticatedProducerRef, command.commandId());

        if (!existing.isEmpty()) {
            var first = existing.get(0);
            if (first.fingerprint().equals(fingerprint)) {
                return new IngressReceipt(DUPLICATE_SAME, first.runId());
            }
            jdbc.update("""
                    insert into ingress_quarantine
                    (tenant_id, authenticated_producer_ref, command_id, received_fingerprint,
                     reason, source_topic, source_partition, source_offset)
                    values (?, ?, ?, ?, 'COMMAND_ID_CONTENT_COLLISION', ?, ?, ?)
                    """, command.tenantId(), authenticatedProducerRef, command.commandId(),
                    fingerprint, position.topic(), position.partition(), position.offset());
            return new IngressReceipt(DUPLICATE_CONFLICT, null);
        }

        String runId = "run-" + UUID.nameUUIDFromBytes((command.tenantId() + "|"
                + authenticatedProducerRef + "|" + command.commandId())
                .getBytes(StandardCharsets.UTF_8));
        jdbc.update("""
                insert into command_inbox
                (tenant_id, authenticated_producer_ref, command_id, payload_fingerprint,
                 disposition, run_id, first_topic, first_partition, first_offset)
                values (?, ?, ?, ?, 'ACCEPTED', ?, ?, ?, ?)
                """, command.tenantId(), authenticatedProducerRef, command.commandId(), fingerprint,
                runId, position.topic(), position.partition(), position.offset());
        jdbc.update("""
                insert into admitted_work
                (tenant_id, authenticated_producer_ref, command_id, run_id, case_id, correlation_id)
                values (?, ?, ?, ?, ?, ?)
                """, command.tenantId(), authenticatedProducerRef, command.commandId(), runId,
                command.caseId(), command.correlationId());
        return new IngressReceipt(ACCEPTED, runId);
    }
    // end::inbox-transaction[]

    @Transactional
    public void quarantineFailure(
            String tenantId,
            String authenticatedProducerRef,
            String commandId,
            String fingerprint,
            String reason,
            KafkaTransportPosition position) {
        jdbc.update("""
                insert into ingress_quarantine
                (tenant_id, authenticated_producer_ref, command_id, received_fingerprint,
                 reason, source_topic, source_partition, source_offset)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """, tenantId, authenticatedProducerRef, commandId, fingerprint, reason,
                position.topic(), position.partition(), position.offset());
    }

    private record Existing(String fingerprint, String runId) {}
}
