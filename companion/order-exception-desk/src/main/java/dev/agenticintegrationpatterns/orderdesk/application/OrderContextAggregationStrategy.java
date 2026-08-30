package dev.agenticintegrationpatterns.orderdesk.application;

import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import dev.agenticintegrationpatterns.orderdesk.model.CaseWork;
import dev.agenticintegrationpatterns.orderdesk.model.OrderContext;
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
