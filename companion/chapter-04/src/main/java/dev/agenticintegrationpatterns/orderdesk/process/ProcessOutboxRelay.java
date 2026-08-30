package dev.agenticintegrationpatterns.orderdesk.process;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.Optional;

public final class ProcessOutboxRelay {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ProcessEventPublisher publisher;
    private final Runnable afterPublish;
    private final Clock clock;

    public ProcessOutboxRelay(
            JdbcTemplate jdbc,
            TransactionTemplate transactions,
            ProcessEventPublisher publisher,
            Runnable afterPublish,
            Clock clock) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.publisher = publisher;
        this.afterPublish = afterPublish;
        this.clock = clock;
    }

    // tag::at-least-once-outbox-relay[]
    public Optional<String> relayOne() {
        var pending = jdbc.query("""
                select event_id, tenant_id, run_id, case_id, aggregate_version,
                       event_type, event_payload, created_at
                  from process_outbox
                 where published_at is null
                 order by created_at, event_id
                """, (rs, row) -> new ProcessOutboxEvent(
                        rs.getString("event_id"), rs.getString("tenant_id"),
                        rs.getString("run_id"), rs.getString("case_id"),
                        rs.getLong("aggregate_version"), rs.getString("event_type"),
                        rs.getString("event_payload"),
                        rs.getObject("created_at", java.time.Instant.class)))
                .stream().findFirst();
        if (pending.isEmpty()) {
            return Optional.empty();
        }

        var event = pending.get();
        publisher.publish(event);
        afterPublish.run(); // a crash here deliberately leaves the row pending
        transactions.executeWithoutResult(status -> jdbc.update("""
                update process_outbox set published_at=?
                 where event_id=? and published_at is null
                """, clock.instant(), event.eventId()));
        return Optional.of(event.eventId());
    }
    // end::at-least-once-outbox-relay[]
}
