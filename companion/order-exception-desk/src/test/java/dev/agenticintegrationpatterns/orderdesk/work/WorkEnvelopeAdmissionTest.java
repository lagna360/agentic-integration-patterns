package dev.agenticintegrationpatterns.orderdesk.work;

import dev.agenticintegrationpatterns.orderdesk.OrderExceptionApplication;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.work.WorkEnvelopeAdmission.TRUSTED_CONTEXT_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(classes = OrderExceptionApplication.class, properties = {
        "orderdesk.work.accepted-uri=mock:work-accepted",
        "orderdesk.work.rejected-uri=mock:work-rejected",
        "camel.springboot.main-run-controller=false",
        "spring.ai.model.chat=none"
})
class WorkEnvelopeAdmissionTest {
    private static final Instant RECEIVED_AT = Instant.parse("2026-08-24T06:14:00Z");

    @Autowired
    ProducerTemplate producer;
    @Autowired
    CamelContext camelContext;

    private String validCommand;

    @BeforeEach
    void reset() throws Exception {
        validCommand = new String(getClass().getResourceAsStream(
                "/fixtures/investigate-order-exception-v1.json").readAllBytes(), StandardCharsets.UTF_8);
        accepted().reset();
        rejected().reset();
    }

    @Test
    void admitsTheCommandAndPreservesDistinctIdentityPurposes() throws Exception {
        accepted().expectedMessageCount(1);

        send(validCommand, trustedContext());

        accepted().assertIsSatisfied(2_000);
        var admitted = accepted().getExchanges().get(0).getMessage()
                .getBody(AdmittedInvestigation.class);
        assertThat(admitted.command().commandId()).isEqualTo("cmd-019483");
        assertThat(admitted.command().caseId())
                .isEqualTo("case-d5a30e20-f10b-38ca-9198-4834746bd37b");
        assertThat(admitted.command().correlationId()).isEqualTo("corr-order-73051");
        assertThat(admitted.command().causedBy()).isEqualTo("evt-019482");
        assertThat(admitted.effectiveCapabilities()).containsExactlyInAnyOrder(
                "read-order", "read-inventory");
        assertThat(admitted.replyContract())
                .isEqualTo(AdmittedInvestigation.ReplyContract.ORDER_EXCEPTION_CASE_RESULTS_V1);
    }

    @Test
    // tag::identity-mismatch-test[]
    void rejectsTenantClaimThatDoesNotMatchAuthenticatedContextBeforeRuntimeHandoff() throws Exception {
        rejected().expectedMessageCount(1);
        accepted().expectedMessageCount(0);

        send(validCommand, new TrustedAdmissionContext(
                "tenant-us", "workload:order-exception-case-manager",
                Set.of("principal:order-ops-ca"), RECEIVED_AT));

        rejected().assertIsSatisfied(2_000);
        accepted().assertIsSatisfied(200);
        assertThat(rejected().getExchanges().get(0).getMessage().getBody())
                .isEqualTo(new RejectedInvestigation(
                        InvalidWorkEnvelopeException.Violation.IDENTITY_MISMATCH,
                        "cmd-019483", "corr-order-73051", "tenant-ca", RECEIVED_AT,
                        "Message identity claims do not match authenticated context"));
        assertThat(rejected().getExchanges().get(0).getMessage().getHeaders())
                .doesNotContainKey(TRUSTED_CONTEXT_HEADER);
        assertThat(rejection().violation())
                .isEqualTo(InvalidWorkEnvelopeException.Violation.IDENTITY_MISMATCH);
    }
    // end::identity-mismatch-test[]

    @Test
    void rejectsExpiredRedeliveryWithoutResettingTheOriginalDeadline() throws Exception {
        rejected().expectedMessageCount(1);

        send(validCommand, new TrustedAdmissionContext(
                "tenant-ca", "workload:order-exception-case-manager",
                Set.of("principal:order-ops-ca"), Instant.parse("2026-08-24T06:24:00Z")));

        rejected().assertIsSatisfied(2_000);
        assertThat(rejection().violation()).isEqualTo(InvalidWorkEnvelopeException.Violation.EXPIRED);
    }

    @Test
    void rejectsUnknownOrEffectingCapabilities() throws Exception {
        rejected().expectedMessageCount(1);
        String effecting = validCommand.replace(
                "\"read-inventory\"", "\"issue-refund\"");

        send(effecting, trustedContext());

        rejected().assertIsSatisfied(2_000);
        assertThat(rejection().violation())
                .isEqualTo(InvalidWorkEnvelopeException.Violation.CAPABILITY_DENIED);
    }

    @Test
    void rejectsStaleOrFutureEvidenceAndAnUnregisteredDynamicReplyDestination() throws Exception {
        rejected().expectedMessageCount(3);
        String staleEvidence = validCommand.replace(
                "2026-08-24T06:18:12Z", "2026-08-24T06:13:30Z");
        String futureEvidence = validCommand.replace(
                "2026-08-24T06:13:12Z", "2026-08-24T06:15:00Z");
        String dynamicReply = validCommand.replace(
                "order-exception-case-results-v1", "kafka:evil-topic?brokers=attacker");

        send(staleEvidence, trustedContext());
        send(futureEvidence, trustedContext());
        send(dynamicReply, trustedContext());

        rejected().assertIsSatisfied(2_000);
        assertThat(rejected().getExchanges())
                .extracting(e -> e.getProperty("CamelExceptionCaught", InvalidWorkEnvelopeException.class).violation())
                .containsExactlyInAnyOrder(
                        InvalidWorkEnvelopeException.Violation.INVALID_EVIDENCE,
                        InvalidWorkEnvelopeException.Violation.INVALID_EVIDENCE,
                        InvalidWorkEnvelopeException.Violation.UNKNOWN_REPLY_CONTRACT);
    }

    @Test
    void rejectsUnsupportedSchemaExcessiveBudgetsAndUnknownConfiguration() throws Exception {
        rejected().expectedMessageCount(3);

        send(validCommand.replace("\"schemaVersion\": 1", "\"schemaVersion\": 2"), trustedContext());
        send(validCommand.replace("\"maxToolRequests\": 8", "\"maxToolRequests\": 80"), trustedContext());
        send(validCommand.replace("order-exception-ca-17", "unknown-policy"), trustedContext());

        rejected().assertIsSatisfied(2_000);
        assertThat(rejected().getExchanges())
                .extracting(e -> e.getProperty("CamelExceptionCaught", InvalidWorkEnvelopeException.class).violation())
                .containsExactlyInAnyOrder(
                        InvalidWorkEnvelopeException.Violation.UNSUPPORTED_CONTRACT,
                        InvalidWorkEnvelopeException.Violation.INVALID_LIMITS,
                        InvalidWorkEnvelopeException.Violation.UNKNOWN_CONFIGURATION);
    }

    @Test
    void strictDecodingRejectsCredentialFieldsTrailingJsonAndNullCapabilitiesWithoutLeakingInput() throws Exception {
        rejected().expectedMessageCount(3);
        String credentialField = validCommand.replace(
                "\"replyContract\":", "\"apiKey\": \"must-not-travel-here\",\n  \"replyContract\":");
        String trailingJson = validCommand + " {}";
        String nullCapability = validCommand.replace("\"read-inventory\"", "null");

        send(credentialField, trustedContext());
        send(trailingJson, trustedContext());
        send(nullCapability, trustedContext());

        rejected().assertIsSatisfied(2_000);
        assertThat(rejected().getExchanges())
                .allSatisfy(exchange -> {
                    assertThat(exchange.getMessage().getBody()).isInstanceOf(RejectedInvestigation.class);
                    assertThat(exchange.getMessage().getBody(RejectedInvestigation.class).violation())
                            .isEqualTo(InvalidWorkEnvelopeException.Violation.MALFORMED);
                    assertThat(exchange.getMessage().getBody().toString())
                            .doesNotContain("must-not-travel-here", "apiKey");
                    assertThat(exchange.getMessage().getHeaders())
                            .doesNotContainKey(TRUSTED_CONTEXT_HEADER);
                });
    }

    @Test
    void admittedWorkDefensivelyCopiesCommandAndTrustedIdentityCollections() throws Exception {
        accepted().expectedMessageCount(1);
        var mutablePrincipals = new HashSet<>(Set.of("principal:order-ops-ca"));
        var context = new TrustedAdmissionContext(
                "tenant-ca", "workload:order-exception-case-manager",
                mutablePrincipals, RECEIVED_AT);
        mutablePrincipals.clear();

        send(validCommand, context);

        accepted().assertIsSatisfied(2_000);
        var admitted = accepted().getExchanges().get(0).getMessage()
                .getBody(AdmittedInvestigation.class);
        assertThat(admitted.trustedContext().permittedPrincipalRefs())
                .containsExactly("principal:order-ops-ca");
        assertThatThrownBy(() -> admitted.command().requestedCapabilities().add("issue-refund"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> admitted.effectiveCapabilities().add("issue-refund"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private void send(String body, TrustedAdmissionContext context) {
        producer.sendBodyAndHeader("direct:admit-investigation", body, TRUSTED_CONTEXT_HEADER, context);
    }

    private TrustedAdmissionContext trustedContext() {
        return new TrustedAdmissionContext(
                "tenant-ca", "workload:order-exception-case-manager",
                Set.of("principal:order-ops-ca"), RECEIVED_AT);
    }

    private InvalidWorkEnvelopeException rejection() {
        return rejected().getExchanges().get(0)
                .getProperty("CamelExceptionCaught", InvalidWorkEnvelopeException.class);
    }

    private MockEndpoint accepted() {
        return camelContext.getEndpoint("mock:work-accepted", MockEndpoint.class);
    }

    private MockEndpoint rejected() {
        return camelContext.getEndpoint("mock:work-rejected", MockEndpoint.class);
    }
}
