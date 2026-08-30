package dev.agenticintegrationpatterns.orderdesk.work;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.work.AdmittedInvestigation.ReplyContract.ORDER_EXCEPTION_CASE_RESULTS_V1;
import static dev.agenticintegrationpatterns.orderdesk.work.InvalidWorkEnvelopeException.Violation;

@Component
public final class WorkEnvelopeAdmission implements Processor {
    public static final String TRUSTED_CONTEXT_HEADER = "trustedAdmissionContext";

    private static final Set<String> ALLOWED_CAPABILITIES = Set.of(
            "read-order", "read-inventory");
    private static final Map<String, AdmittedInvestigation.ReplyContract> REPLY_CONTRACTS = Map.of(
            "order-exception-case-results-v1", ORDER_EXCEPTION_CASE_RESULTS_V1);

    @Override
    // tag::admission[]
    public void process(Exchange exchange) {
        var command = exchange.getMessage().getBody(InvestigateOrderException.class);
        var trusted = exchange.getMessage().getHeader(
                TRUSTED_CONTEXT_HEADER, TrustedAdmissionContext.class);
        if (trusted == null) {
            reject(Violation.IDENTITY_MISMATCH, "Trusted admission context is required");
        }

        validateContract(command);
        validateIdentity(command, trusted);
        validateTime(command, trusted.receivedAt());
        validateLimits(command.limits());
        Set<String> effectiveCapabilities = validateCapabilities(command.requestedCapabilities());
        validateEvidence(command, trusted.receivedAt());
        validateConfiguration(command.configuration());
        var reply = REPLY_CONTRACTS.get(command.replyContract());
        if (reply == null) {
            reject(Violation.UNKNOWN_REPLY_CONTRACT, "Reply contract is not registered");
        }

        exchange.getMessage().setBody(new AdmittedInvestigation(
                command, trusted, effectiveCapabilities, command.limits(), reply));
        exchange.getMessage().setHeader("commandId", command.commandId());
        exchange.getMessage().setHeader("correlationId", command.correlationId());
        exchange.getMessage().setHeader("tenantId", trusted.tenantId());
    }
    // end::admission[]

    private static void validateContract(InvestigateOrderException command) {
        if (command == null || command.schemaVersion() != 1
                || !"InvestigateOrderException".equals(command.type())
                || command.objective() == null
                || !"investigate-order-exception".equals(command.objective().code())
                || !"v1".equals(command.objective().version())) {
            reject(Violation.UNSUPPORTED_CONTRACT, "Unsupported schema, type, or objective");
        }
        if (blank(command.commandId()) || blank(command.caseId())
                || blank(command.correlationId()) || blank(command.causedBy())
                || command.commandId().equals(command.causedBy())) {
            reject(Violation.INVALID_IDENTITY, "Message and causal identifiers must be distinct and nonblank");
        }
    }

    private static void validateIdentity(
            InvestigateOrderException command, TrustedAdmissionContext trusted) {
        if (blank(command.tenantId()) || blank(command.principalRef())
                || blank(command.requestingWorkloadRef())
                || !command.tenantId().equals(trusted.tenantId())
                || !command.requestingWorkloadRef().equals(trusted.authenticatedWorkloadRef())
                || trusted.permittedPrincipalRefs() == null
                || !trusted.permittedPrincipalRefs().contains(command.principalRef())) {
            reject(Violation.IDENTITY_MISMATCH, "Message identity claims do not match authenticated context");
        }
    }

    private static void validateTime(InvestigateOrderException command, Instant receivedAt) {
        if (command.issuedAt() == null || command.deadlineAt() == null || receivedAt == null
                || command.issuedAt().isAfter(receivedAt)
                || !command.deadlineAt().isAfter(receivedAt)) {
            reject(Violation.EXPIRED, "Command is expired or has invalid time bounds");
        }
    }

    private static void validateLimits(InvestigateOrderException.ExecutionLimits limits) {
        if (limits == null || limits.maxModelTurns() < 1 || limits.maxModelTurns() > 6
                || limits.maxToolRequests() < 0 || limits.maxToolRequests() > 8
                || limits.maxOutputTokens() < 1 || limits.maxOutputTokens() > 4_000) {
            reject(Violation.INVALID_LIMITS, "Requested limits exceed application ceilings");
        }
    }

    private static Set<String> validateCapabilities(Set<String> requested) {
        if (requested == null || requested.isEmpty() || requested.stream().anyMatch(WorkEnvelopeAdmission::blank)
                || !ALLOWED_CAPABILITIES.containsAll(requested)) {
            reject(Violation.CAPABILITY_DENIED, "A requested capability is not allowed");
        }
        return Set.copyOf(requested);
    }

    private static Set<String> validateCapabilities(java.util.List<String> requested) {
        if (requested == null || requested.stream().anyMatch(WorkEnvelopeAdmission::blank)
                || requested.size() != new HashSet<>(requested).size()) {
            reject(Violation.CAPABILITY_DENIED, "Capabilities must be unique");
        }
        return validateCapabilities(new HashSet<>(requested));
    }

    private static void validateEvidence(InvestigateOrderException command, Instant receivedAt) {
        if (command.evidence() == null || command.evidence().isEmpty()) {
            reject(Violation.INVALID_EVIDENCE, "At least one evidence reference is required");
        }
        var references = new HashSet<String>();
        for (var evidence : command.evidence()) {
            if (evidence == null || blank(evidence.reference()) || blank(evidence.sourceSystem())
                    || blank(evidence.sourceVersion()) || evidence.observedAt() == null
                    || evidence.validUntil() == null || evidence.trust() == null
                    || evidence.observedAt().isAfter(receivedAt)
                    || !evidence.observedAt().isBefore(evidence.validUntil())
                    || !evidence.validUntil().isAfter(receivedAt)
                    || !references.add(evidence.reference())) {
                reject(Violation.INVALID_EVIDENCE, "Evidence reference is incomplete, stale, or duplicated");
            }
        }
    }

    private static void validateConfiguration(
            InvestigateOrderException.ConfigurationReferences configuration) {
        if (configuration == null
                || !"order-exception-investigation-v1".equals(configuration.instructionSetRef())
                || !"order-exception-ca-17".equals(configuration.policySetRef())
                || !"order-desk-capabilities-v1".equals(configuration.capabilityCatalogRef())) {
            reject(Violation.UNKNOWN_CONFIGURATION, "Configuration reference is not supported");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static void reject(Violation violation, String message) {
        throw new InvalidWorkEnvelopeException(violation, message);
    }
}
