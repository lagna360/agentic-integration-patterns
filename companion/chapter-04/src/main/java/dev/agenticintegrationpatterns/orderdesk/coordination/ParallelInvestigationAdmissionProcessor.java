package dev.agenticintegrationpatterns.orderdesk.coordination;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public final class ParallelInvestigationAdmissionProcessor implements Processor {
    public static final String PLAN_PROPERTY = "orderdeskParallelPlan";
    public static final String RECIPIENTS_PROPERTY = "orderdeskParallelRecipients";

    private final ParallelInvestigationPlanProvider plans;
    private final InvestigationRecipientRegistry recipients;

    public ParallelInvestigationAdmissionProcessor(
            ParallelInvestigationPlanProvider plans,
            InvestigationRecipientRegistry recipients) {
        this.plans = plans;
        this.recipients = recipients;
    }

    @Override
    public void process(Exchange exchange) {
        var request = exchange.getMessage().getBody(ParallelInvestigationRequest.class);
        var plan = plans.plan(request);
        exchange.getMessage().removeHeaders("*");
        exchange.setProperty(PLAN_PROPERTY, plan);
        exchange.setProperty(RECIPIENTS_PROPERTY, recipients.endpoints(plan));
        exchange.getMessage().setBody(new ParallelBranchRequest(request.context(), plan));
    }
}
