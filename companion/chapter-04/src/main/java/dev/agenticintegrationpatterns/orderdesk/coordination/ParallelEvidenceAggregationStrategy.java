package dev.agenticintegrationpatterns.orderdesk.coordination;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.time.Clock;

import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.CompletionTrigger.TIMEOUT;

@Component
public final class ParallelEvidenceAggregationStrategy implements AggregationStrategy {
    private final ParallelEvidenceReducer reducer;
    private final Clock clock;

    public ParallelEvidenceAggregationStrategy(
            ParallelEvidenceReducer reducer,
            Clock clock) {
        this.reducer = reducer;
        this.clock = clock;
    }

    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (newExchange.getException() != null) {
            throw new IllegalStateException(
                    "Parallel investigation branch failed unexpectedly",
                    newExchange.getException());
        }
        Exchange carrier = oldExchange == null ? newExchange : oldExchange;
        if (carrier.getMessage().getBody() instanceof ParallelEvidenceSet) {
            return carrier;
        }
        var plan = newExchange.getProperty(
                ParallelInvestigationAdmissionProcessor.PLAN_PROPERTY,
                ParallelInvestigationPlan.class);
        var accumulator = oldExchange == null
                ? reducer.start(plan)
                : oldExchange.getMessage().getBody(ParallelEvidenceAccumulator.class);
        var reply = newExchange.getMessage().getBody(InvestigationReply.class);
        var merged = reducer.merge(accumulator, reply, clock.instant());
        carrier.getMessage().removeHeaders("*");
        carrier.getMessage().setBody(merged.accumulator());
        carrier.setProperty(ParallelInvestigationAdmissionProcessor.PLAN_PROPERTY, plan);
        return carrier;
    }

    @Override
    public void timeout(Exchange exchange, int index, int total, long timeout) {
        if (exchange != null
                && exchange.getMessage().getBody() instanceof ParallelEvidenceAccumulator current) {
            exchange.getMessage().setBody(reducer.close(current, clock.instant(), TIMEOUT));
        }
    }
}
