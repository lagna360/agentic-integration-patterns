package dev.agenticintegrationpatterns.orderdesk.failure;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public final class FailureClassificationProcessor implements Processor {
    private final FailureClassifier classifier;

    public FailureClassificationProcessor(FailureClassifier classifier) {
        this.classifier = classifier;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        var observation = exchange.getMessage().getMandatoryBody(ClassificationObservation.class);
        exchange.getMessage().setBody(classifier.classify(observation));
    }
}
