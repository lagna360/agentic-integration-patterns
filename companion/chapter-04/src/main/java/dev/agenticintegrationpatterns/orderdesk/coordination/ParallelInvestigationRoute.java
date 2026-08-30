package dev.agenticintegrationpatterns.orderdesk.coordination;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;

import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationBranch.INVENTORY_RECHECK;
import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationBranch.ORDER_HISTORY;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.Disposition.EVIDENCE_READY;

@Component
public final class ParallelInvestigationRoute extends RouteBuilder {
    private final ParallelInvestigationAdmissionProcessor admission;
    private final ParallelEvidenceAggregationStrategy aggregation;
    private final ParallelEvidenceFinalizer finalizer;
    private final ParallelInvestigator investigator;
    private final long timeoutMillis;

    public ParallelInvestigationRoute(
            ParallelInvestigationAdmissionProcessor admission,
            ParallelEvidenceAggregationStrategy aggregation,
            ParallelEvidenceFinalizer finalizer,
            ParallelInvestigator investigator,
            @Value("${orderdesk.parallel.timeout-ms:250}") long timeoutMillis) {
        this.admission = admission;
        this.aggregation = aggregation;
        this.finalizer = finalizer;
        this.investigator = investigator;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    // tag::parallel-route[]
    public void configure() {
        ExecutorService workers = getContext().getExecutorServiceManager()
                .newFixedThreadPool(this, "parallel-investigation", 2);

        from("direct:parallel-investigation")
                .routeId("parallel-investigation-scatter-gather")
                .process(admission)
                .recipientList(exchangeProperty(
                        ParallelInvestigationAdmissionProcessor.RECIPIENTS_PROPERTY))
                    .aggregationStrategy(aggregation)
                    .executorService(workers)
                    .parallelProcessing()
                    .synchronous()
                    .timeout(timeoutMillis)
                    .stopOnException()
                    .allowedSchemes("direct")
                .end()
                .process(finalizer)
                .choice()
                    .when(exchange -> exchange.getMessage()
                            .getBody(ParallelEvidenceSet.class).disposition()
                            == EVIDENCE_READY)
                        .to("seda:parallel-evidence-ready?waitForTaskToComplete=Never")
                    .otherwise()
                        .to("seda:parallel-evidence-review?waitForTaskToComplete=Never");

        from("direct:parallel-inventory-recheck")
                .routeId("parallel-inventory-recheck")
                .process(exchange -> reply(exchange, INVENTORY_RECHECK));

        from("direct:parallel-order-history")
                .routeId("parallel-order-history")
                .process(exchange -> reply(exchange, ORDER_HISTORY));
    }
    // end::parallel-route[]

    private void reply(org.apache.camel.Exchange exchange, InvestigationBranch branch) {
        var request = exchange.getMessage().getBody(ParallelBranchRequest.class);
        exchange.getMessage().setBody(investigator.investigate(branch, request));
    }
}
