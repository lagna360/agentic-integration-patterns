package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.AssessmentRequest;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public class AssessAndValidateProcessor implements Processor {
    private final FailureAssessmentGateway gateway;
    private final AssessmentValidator validator;

    public AssessAndValidateProcessor(FailureAssessmentGateway gateway, AssessmentValidator validator) {
        this.gateway = gateway;
        this.validator = validator;
    }

    @Override
    public void process(Exchange exchange) {
        var request = exchange.getMessage().getBody(AssessmentRequest.class);
        exchange.getMessage().setBody(validator.validate(request, gateway.assess(request)));
    }
}
