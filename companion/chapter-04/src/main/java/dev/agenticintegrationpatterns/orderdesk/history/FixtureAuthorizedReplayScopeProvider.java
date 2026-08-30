package dev.agenticintegrationpatterns.orderdesk.history;

import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Set;

/** Teaching adapter; production derives this scope from authenticated operator/service context. */
@Component
public class FixtureAuthorizedReplayScopeProvider {
    private final Clock clock;
    private volatile AuthorizedReplayScope current;

    public FixtureAuthorizedReplayScopeProvider(Clock clock) {
        this.clock = clock;
        reset();
    }

    public AuthorizedReplayScope current() {
        return current;
    }

    public void reset() {
        current = new AuthorizedReplayScope(
                "actor:recovery-owner-19", Set.of("tenant-ca"),
                Set.of("INCIDENT_REVIEW", "MODEL_EVALUATION"),
                JdbcReplayManager.AUDIENCE, JdbcReplayManager.SERVICE,
                clock.instant().plusSeconds(3_600));
    }

    public void useForTest(AuthorizedReplayScope scope) {
        current = scope;
    }
}
