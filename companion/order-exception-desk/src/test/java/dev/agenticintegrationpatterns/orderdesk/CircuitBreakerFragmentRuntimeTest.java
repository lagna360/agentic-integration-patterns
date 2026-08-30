package dev.agenticintegrationpatterns.orderdesk;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

class CircuitBreakerFragmentRuntimeTest {
    @Test
    void chapterThreeFallbackReceivesAProviderFailure() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.getRegistry().bind("modelGateway", (org.apache.camel.Processor) exchange -> {
                throw new IllegalStateException("simulated provider failure");
            });
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:modelAssessment")
                        .circuitBreaker()
                            .to("bean:modelGateway")
                        .onFallback()
                            .to("mock:modelUnavailable")
                        .end();
                }
            });
            context.start();
            MockEndpoint fallback = context.getEndpoint("mock:modelUnavailable", MockEndpoint.class);
            fallback.expectedMessageCount(1);

            context.createProducerTemplate().sendBody("direct:modelAssessment", "case-1");

            fallback.assertIsSatisfied(2_000);
        }
    }
}
