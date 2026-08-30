package dev.agenticintegrationpatterns.orderdesk.route;

import dev.agenticintegrationpatterns.orderdesk.application.AssessAndValidateProcessor;
import dev.agenticintegrationpatterns.orderdesk.application.AssessmentGatewayException;
import dev.agenticintegrationpatterns.orderdesk.application.AssessmentPredicates;
import dev.agenticintegrationpatterns.orderdesk.application.EventDecoder;
import dev.agenticintegrationpatterns.orderdesk.application.EventValidator;
import dev.agenticintegrationpatterns.orderdesk.application.FailureProcessor;
import dev.agenticintegrationpatterns.orderdesk.application.FixtureOrderContextProvider;
import dev.agenticintegrationpatterns.orderdesk.application.InMemoryAuditRecorder;
import dev.agenticintegrationpatterns.orderdesk.application.InMemoryCaseStore;
import dev.agenticintegrationpatterns.orderdesk.application.InvalidAssessmentException;
import dev.agenticintegrationpatterns.orderdesk.application.InvalidEventException;
import dev.agenticintegrationpatterns.orderdesk.application.MalformedEventException;
import dev.agenticintegrationpatterns.orderdesk.application.OrderContextAggregationStrategy;
import dev.agenticintegrationpatterns.orderdesk.application.OutcomeFactory;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class OrderExceptionRoute extends RouteBuilder {
    private final EventDecoder eventDecoder;
    private final EventValidator eventValidator;
    private final InMemoryCaseStore caseStore;
    private final FixtureOrderContextProvider contextProvider;
    private final OrderContextAggregationStrategy contextAggregation;
    private final AssessAndValidateProcessor assessAndValidate;
    private final AssessmentPredicates predicates;
    private final OutcomeFactory outcomes;
    private final FailureProcessor failures;
    private final InMemoryAuditRecorder auditRecorder;

    @Value("${orderdesk.routes.input-uri}")
    private String inputUri;
    @Value("${orderdesk.routes.proposed-uri}")
    private String proposedUri;
    @Value("${orderdesk.routes.manual-review-uri}")
    private String manualReviewUri;
    @Value("${orderdesk.routes.invalid-uri}")
    private String invalidUri;
    @Value("${orderdesk.routes.assessment-failed-uri}")
    private String assessmentFailedUri;

    public OrderExceptionRoute(
            EventDecoder eventDecoder,
            EventValidator eventValidator,
            InMemoryCaseStore caseStore,
            FixtureOrderContextProvider contextProvider,
            OrderContextAggregationStrategy contextAggregation,
            AssessAndValidateProcessor assessAndValidate,
            AssessmentPredicates predicates,
            OutcomeFactory outcomes,
            FailureProcessor failures,
            InMemoryAuditRecorder auditRecorder) {
        this.eventDecoder = eventDecoder;
        this.eventValidator = eventValidator;
        this.caseStore = caseStore;
        this.contextProvider = contextProvider;
        this.contextAggregation = contextAggregation;
        this.assessAndValidate = assessAndValidate;
        this.predicates = predicates;
        this.outcomes = outcomes;
        this.failures = failures;
        this.auditRecorder = auditRecorder;
    }

    @Override
    public void configure() {
        errorHandler(defaultErrorHandler().maximumRedeliveries(0));

        onException(MalformedEventException.class, InvalidEventException.class)
                .handled(true)
                .process(failures.invalidMessage())
                .process(auditRecorder)
                .to(invalidUri);

        onException(AssessmentGatewayException.class, InvalidAssessmentException.class)
                .handled(true)
                .process(failures.assessmentFailure())
                .process(auditRecorder)
                .to(assessmentFailedUri);

        from("direct:load-order-context")
                .routeId("load-order-context")
                .process(contextProvider);

        // tag::route[]
        from(inputUri)
                .routeId("inventory-shortfall-assessment")
                .process(eventDecoder)
                .process(eventValidator)
                .process(caseStore)
                .enrich("direct:load-order-context", contextAggregation)
                .process(assessAndValidate)
                .choice()
                    .when(predicates.proposesResolution())
                        .process(outcomes.proposal())
                        .process(auditRecorder)
                        .to(proposedUri)
                    .otherwise()
                        .process(outcomes.manualReview())
                        .process(auditRecorder)
                        .to(manualReviewUri)
                .end();
        // end::route[]
    }
}
