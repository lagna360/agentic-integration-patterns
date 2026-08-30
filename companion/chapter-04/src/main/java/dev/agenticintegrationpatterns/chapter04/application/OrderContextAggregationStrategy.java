package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.AssessmentRequest;
import dev.agenticintegrationpatterns.chapter04.model.CaseWork;
import dev.agenticintegrationpatterns.chapter04.model.OrderContext;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class OrderContextAggregationStrategy implements AggregationStrategy {
    @Override
    public Exchange aggregate(Exchange original, Exchange resource) {
        var work = original.getMessage().getBody(CaseWork.class);
        var context = resource.getMessage().getBody(OrderContext.class);
        original.getMessage().setBody(new AssessmentRequest(work, context));
        return original;
    }
}
