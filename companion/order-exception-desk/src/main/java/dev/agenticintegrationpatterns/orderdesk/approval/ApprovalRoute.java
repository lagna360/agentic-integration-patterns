package dev.agenticintegrationpatterns.orderdesk.approval;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class ApprovalRoute extends RouteBuilder {
    private final GuardedEffectService effects;

    public ApprovalRoute(GuardedEffectService effects) {
        this.effects = effects;
    }

    @Override
    public void configure() {
        // tag::approval-effect-route[]
        from("direct:register-authorized-effect")
                .routeId("register-authorized-effect")
                .bean(effects, "registerAuthorized");

        from("direct:execute-authorized-effect")
                .routeId("execute-authorized-effect")
                .bean(effects, "executeAuthorized");
        // end::approval-effect-route[]
    }
}
