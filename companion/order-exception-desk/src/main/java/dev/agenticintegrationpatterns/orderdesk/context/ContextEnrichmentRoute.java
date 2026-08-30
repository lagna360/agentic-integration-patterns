package dev.agenticintegrationpatterns.orderdesk.context;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public final class ContextEnrichmentRoute extends RouteBuilder {
    private final ContextResolutionProcessor resolution;
    private final ContextFailureProcessor failure;

    public ContextEnrichmentRoute(
            ContextResolutionProcessor resolution,
            ContextFailureProcessor failure) {
        this.resolution = resolution;
        this.failure = failure;
    }

    @Override
    // tag::context-route[]
    public void configure() {
        onException(ContextResolutionException.class)
                .handled(true)
                .process(failure)
                .to("seda:context-unavailable");

        from("direct:resolve-investigation-context")
                .routeId("resolve-investigation-context")
                .process(resolution)
                .to("seda:context-ready");
    }
    // end::context-route[]
}
