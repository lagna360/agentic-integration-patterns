package dev.agenticintegrationpatterns.orderdesk.capability;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public final class CapabilityFailureProcessor implements Processor {
    private static final Pattern VALID_CALL_ID = Pattern.compile("[A-Za-z0-9._:-]{1,128}");

    @Override
    public void process(Exchange exchange) {
        var failure = exchange.getProperty(
                Exchange.EXCEPTION_CAUGHT, CapabilityGatewayException.class);
        var request = exchange.getMessage().getBody(CapabilityInvocationRequest.class);
        String runId = request == null || request.context() == null
                || request.context().snapshot() == null
                ? null : request.context().snapshot().runId();
        String tenantId = request == null || request.context() == null
                || request.context().snapshot() == null
                ? null : request.context().snapshot().tenantId();
        String candidateCallId = request == null || request.intent() == null
                ? null : request.intent().callId();
        String callId = candidateCallId != null && VALID_CALL_ID.matcher(candidateCallId).matches()
                ? candidateCallId : null;
        exchange.getMessage().removeHeaders("*");
        exchange.getMessage().setBody(new CapabilityUnavailable(
                runId, tenantId, callId,
                failure == null ? CapabilityGatewayException.Reason.INVALID_INTENT
                        : failure.reason()));
    }
}
