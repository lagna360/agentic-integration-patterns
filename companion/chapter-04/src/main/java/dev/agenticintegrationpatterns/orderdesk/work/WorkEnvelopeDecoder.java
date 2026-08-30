package dev.agenticintegrationpatterns.orderdesk.work;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;

@Component
public final class WorkEnvelopeDecoder implements Processor {
    private final ObjectMapper objectMapper;

    public WorkEnvelopeDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void process(Exchange exchange) {
        try {
            var command = objectMapper.readerFor(InvestigateOrderException.class)
                    .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readValue(exchange.getMessage().getBody(String.class));
            exchange.getMessage().setBody(command);
        } catch (Exception malformed) {
            throw new InvalidWorkEnvelopeException(
                    InvalidWorkEnvelopeException.Violation.MALFORMED,
                    "Payload is not a valid InvestigateOrderException command");
        }
    }
}
