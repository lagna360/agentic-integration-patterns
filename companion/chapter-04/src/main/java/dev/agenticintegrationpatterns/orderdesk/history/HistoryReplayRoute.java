package dev.agenticintegrationpatterns.orderdesk.history;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class HistoryReplayRoute extends RouteBuilder {
    private final JdbcCaseHistory history;
    private final JdbcReplayManager replay;
    private final FixtureAuthorizedReplayScopeProvider replayScopes;

    public HistoryReplayRoute(JdbcCaseHistory history, JdbcReplayManager replay,
            FixtureAuthorizedReplayScopeProvider replayScopes) {
        this.history = history;
        this.replay = replay;
        this.replayScopes = replayScopes;
    }

    @Override
    public void configure() {
        // tag::history-replay-routes[]
        from("direct:record-history-observation")
                .routeId("record-history-observation")
                .bean(history, "record");

        from("direct:execute-authorized-replay")
                .routeId("execute-authorized-replay")
                .process(exchange -> exchange.getMessage().setBody(replay.execute(
                        replayScopes.current(), exchange.getMessage().getBody(ReplayCommand.class))));
        // There is deliberately no route from replay to a tool or effect adapter.
        // end::history-replay-routes[]
    }
}
