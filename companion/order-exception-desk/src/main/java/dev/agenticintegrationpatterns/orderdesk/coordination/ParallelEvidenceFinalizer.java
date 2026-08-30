package dev.agenticintegrationpatterns.orderdesk.coordination;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.time.Clock;

import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.CompletionTrigger.ALL_REPLIES;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.CompletionTrigger.TIMEOUT;

@Component
public final class ParallelEvidenceFinalizer implements Processor {
    private final ParallelEvidenceReducer reducer;
    private final Clock clock;

    public ParallelEvidenceFinalizer(ParallelEvidenceReducer reducer, Clock clock) {
        this.reducer = reducer;
        this.clock = clock;
    }

    @Override
    public void process(Exchange exchange) {
        Object body = exchange.getMessage().getBody();
        ParallelEvidenceSet result;
        if (body instanceof ParallelEvidenceSet completed) {
            result = completed;
        } else if (body instanceof ParallelEvidenceAccumulator current) {
            result = reducer.close(current, clock.instant(), ALL_REPLIES);
        } else {
            var plan = exchange.getProperty(
                    ParallelInvestigationAdmissionProcessor.PLAN_PROPERTY,
                    ParallelInvestigationPlan.class);
            result = reducer.close(reducer.start(plan), clock.instant(), TIMEOUT);
        }
        exchange.getMessage().removeHeaders("*");
        exchange.getMessage().setBody(result);
    }
}
