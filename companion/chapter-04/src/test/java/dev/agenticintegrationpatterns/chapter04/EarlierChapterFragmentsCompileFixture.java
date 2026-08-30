package dev.agenticintegrationpatterns.chapter04;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.IdempotentRepository;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compile fixtures for the illustrative Camel fragments printed in Chapters 1 and 3.
 * External endpoint components are intentionally not started here.
 */
class EarlierChapterFragmentsCompileTest {
    @Test
    void chapterFragmentsRemainValidJavaDsl() {
        assertThat(new ChapterOneRoutes()).isInstanceOf(RouteBuilder.class);
        assertThat(new ChapterThreeEnvelope(null, null)).isInstanceOf(RouteBuilder.class);
        assertThat(new ChapterThreeCircuitBreaker()).isInstanceOf(RouteBuilder.class);
    }

    static final class ChapterOneRoutes extends RouteBuilder {
        @Override
        public void configure() {
            from("kafka:inventory-shortfalls")
                .to("direct:update-exception-case")
                .to("direct:build-investigation-command")
                .to("kafka:investigate-order-exception");

            from("kafka:investigate-order-exception")
                .to("direct:bounded-investigation")
                .to("kafka:resolution-proposals");
        }
    }

    static final class ChapterThreeEnvelope extends RouteBuilder {
        private final IdempotentRepository persistentIdempotentRepository;
        private final AggregationStrategy orderContextAggregationStrategy;

        ChapterThreeEnvelope(
                IdempotentRepository persistentIdempotentRepository,
                AggregationStrategy orderContextAggregationStrategy) {
            this.persistentIdempotentRepository = persistentIdempotentRepository;
            this.orderContextAggregationStrategy = orderContextAggregationStrategy;
        }

        @Override
        public void configure() {
            errorHandler(deadLetterChannel("jms:queue:investigation.dead")
                .maximumRedeliveries(2)
                .redeliveryDelay(1_000));

            from("jms:queue:investigation.in")
                .routeId("order-investigation-envelope")
                .validate(header("eventId").isNotNull())
                .validate(header("correlationId").isNotNull())
                .bean("orderExceptionNormalizer")
                .idempotentConsumer(
                    header("eventId"), persistentIdempotentRepository)
                .enrich(
                    "direct:loadPermittedEvidence",
                    orderContextAggregationStrategy)
                .to("bean:boundedInvestigator")
                .bean("proposalValidator")
                .bean("policyGate")
                .wireTap("seda:operationalTelemetry")
                .to("jms:queue:resolution.proposed");
        }
    }

    static final class ChapterThreeCircuitBreaker extends RouteBuilder {
        @Override
        public void configure() {
            from("direct:modelAssessment")
                .circuitBreaker()
                    .to("bean:modelGateway")
                .onFallback()
                    .to("seda:modelUnavailable")
                .end();
        }
    }
}
