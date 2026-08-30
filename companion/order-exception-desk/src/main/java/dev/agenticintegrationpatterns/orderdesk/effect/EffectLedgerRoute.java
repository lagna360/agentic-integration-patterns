package dev.agenticintegrationpatterns.orderdesk.effect;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import static dev.agenticintegrationpatterns.orderdesk.effect.EffectReceipt.State.*;

@Component
public class EffectLedgerRoute extends RouteBuilder {
    private final JdbcEffectLedger ledger;
    private final EffectExecutionService execution;

    public EffectLedgerRoute(JdbcEffectLedger ledger, EffectExecutionService execution) {
        this.ledger = ledger;
        this.execution = execution;
    }

    @Override
    public void configure() {
        // tag::effect-ledger-route[]
        from("direct:record-effect")
                .routeId("record-effect")
                .bean(ledger, "register");

        from("direct:execute-recorded-effect")
                .routeId("execute-recorded-effect")
                .bean(execution, "executeOne")
                .choice()
                    .when(simple("${body.state} == 'UNKNOWN'"))
                        .to("seda:effect-reconciliation?waitForTaskToComplete=Never")
                    .when(simple("${body.state} == 'SUCCEEDED'"))
                        .to("seda:effect-succeeded?waitForTaskToComplete=Never")
                    .when(simple("${body.state} == 'FAILED_CONFIRMED'"))
                        .to("seda:effect-failed-confirmed?waitForTaskToComplete=Never")
                    .when(simple("${body.state} == 'ACCEPTED'"))
                        .to("seda:effect-accepted?waitForTaskToComplete=Never")
                .end();

        from("direct:execute-split-shipment-effect")
                .routeId("execute-split-shipment-effect")
                .bean(execution, "executeSplitShipment");

        from("direct:execute-reservation-release-effect")
                .routeId("execute-reservation-release-effect")
                .bean(execution, "executeRelease");
        // end::effect-ledger-route[]
    }
}
