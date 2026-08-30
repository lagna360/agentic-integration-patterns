package dev.agenticintegrationpatterns.orderdesk.peer;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class RemotePeerRoute extends RouteBuilder {
    private final JdbcRemotePeerTaskManager manager;
    private final FixtureProtectedRemotePeerContextProvider contexts;

    public RemotePeerRoute(JdbcRemotePeerTaskManager manager,
            FixtureProtectedRemotePeerContextProvider contexts) {
        this.manager = manager;
        this.contexts = contexts;
    }

    @Override
    public void configure() {
        // tag::remote-peer-route[]
        from("direct:open-remote-peer-task")
                .routeId("open-remote-peer-task")
                .bean(manager, "open");

        from("direct:accept-remote-peer-update")
                .routeId("accept-remote-peer-update")
                .process(exchange -> exchange.getMessage().setBody(manager.accept(
                        contexts.current(),
                        exchange.getMessage().getBody(RemotePeerUpdate.class))));

        from("direct:request-remote-peer-cancellation")
                .routeId("request-remote-peer-cancellation")
                .bean(manager, "requestCancellation");
        // end::remote-peer-route[]
    }
}
