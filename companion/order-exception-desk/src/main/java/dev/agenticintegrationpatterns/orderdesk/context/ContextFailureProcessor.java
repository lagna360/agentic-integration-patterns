package dev.agenticintegrationpatterns.orderdesk.context;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public final class ContextFailureProcessor implements Processor {
    @Override
    public void process(Exchange exchange) {
        var failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT,
                ContextResolutionException.class);
        var request = exchange.getMessage().getBody(ContextResolutionRequest.class);
        String runId = request == null ? null : request.runId();
        String tenantId = request == null || request.admitted() == null
                || request.admitted().trustedContext() == null
                ? null : request.admitted().trustedContext().tenantId();
        exchange.getMessage().setBody(new ContextUnavailable(
                runId, tenantId, failure == null
                        ? ContextResolutionException.Reason.INVALID_REQUEST
                        : failure.reason()));
        exchange.getMessage().removeHeaders("*");
    }
}
