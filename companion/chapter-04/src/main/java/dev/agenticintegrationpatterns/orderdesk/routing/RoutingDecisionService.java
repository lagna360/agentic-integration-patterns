package dev.agenticintegrationpatterns.orderdesk.routing;

import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision.Reason.*;
import static dev.agenticintegrationpatterns.orderdesk.routing.RoutingDecision.Target.*;

@Component
public final class RoutingDecisionService {
    private final Clock clock;
    private final RoutingPolicyProvider policyProvider;

    public RoutingDecisionService(Clock clock, RoutingPolicyProvider policyProvider) {
        this.clock = clock;
        this.policyProvider = policyProvider;
    }

    // tag::routing-decision[]
    public RoutingDecision decide(InvestigationRoutingRequest request) {
        if (request == null || request.context() == null
                || request.context().snapshot() == null
                || request.context().admitted() == null) {
            return decision(request, MANUAL_REVIEW, INVALID_ASSESSMENT, null, null, null);
        }
        var command = request.context().admitted().command();
        // Policy is resolved by trusted application code, never accepted with model advice.
        var policy = policyProvider.currentPolicy(request.context());
        if (command.deadlineAt() == null || !command.deadlineAt().isAfter(clock.instant())) {
            return decision(request, INVESTIGATION_STOPPED, DEADLINE_EXCEEDED,
                    policyVersion(policy), advisoryClass(request), advisoryScore(request));
        }
        if (!validPolicy(policy)) {
            return decision(request, MANUAL_REVIEW, INVALID_POLICY,
                    policyVersion(policy), advisoryClass(request), advisoryScore(request));
        }
        if (policy.forceManualReview()) {
            return decision(request, MANUAL_REVIEW, POLICY_OVERRIDE,
                    policyVersion(policy), advisoryClass(request), advisoryScore(request));
        }
        var assessment = request.assessment();
        if (!validAssessment(assessment)) {
            return decision(request, MANUAL_REVIEW, INVALID_ASSESSMENT,
                    policy.policyVersion(), advisoryClass(request), advisoryScore(request));
        }

        Set<String> availableEvidence = availableEvidence(request);
        if (assessment.evidenceIds().stream().anyMatch(id -> !availableEvidence.contains(id))) {
            return decision(request, MANUAL_REVIEW, UNVERIFIED_EVIDENCE,
                    policy.policyVersion(), assessment.recommendedClass(),
                    assessment.supportScore());
        }
        if (assessment.supportScore() < policy.minimumSupportScore()) {
            return decision(request, MANUAL_REVIEW, BELOW_SUPPORT_THRESHOLD,
                    policy.policyVersion(), assessment.recommendedClass(),
                    assessment.supportScore());
        }

        RoutingDecision.Target target = switch (assessment.recommendedClass()) {
            case "INVENTORY_FOLLOW_UP" -> INVENTORY_FOLLOW_UP;
            case "ORDER_FOLLOW_UP" -> ORDER_FOLLOW_UP;
            case "EVIDENCE_SUFFICIENT" -> READY_FOR_ASSESSMENT;
            default -> null;
        };
        if (target == null || !policy.allowedTargets().contains(target)
                || !grantAllows(request, target)) {
            return decision(request, MANUAL_REVIEW, TARGET_NOT_ALLOWED,
                    policy.policyVersion(), assessment.recommendedClass(),
                    assessment.supportScore());
        }
        return decision(request, target, ADVISORY_ACCEPTED,
                policy.policyVersion(), assessment.recommendedClass(),
                assessment.supportScore());
    }
    // end::routing-decision[]

    private static boolean validPolicy(RoutingPolicyContext policy) {
        return policy != null && policy.policyVersion() != null
                && !policy.policyVersion().isBlank()
                && Double.isFinite(policy.minimumSupportScore())
                && policy.minimumSupportScore() >= 0.0
                && policy.minimumSupportScore() <= 1.0
                && policy.allowedTargets() != null;
    }

    private static boolean validAssessment(AdvisoryRoutingAssessment assessment) {
        return assessment != null
                && assessment.recommendedClass() != null
                && !assessment.recommendedClass().isBlank()
                && Double.isFinite(assessment.supportScore())
                && assessment.supportScore() >= 0.0
                && assessment.supportScore() <= 1.0
                && assessment.evidenceIds() != null
                && !assessment.evidenceIds().isEmpty()
                && assessment.evidenceIds().stream().noneMatch(
                        value -> value == null || value.isBlank())
                && assessment.evidenceIds().size()
                        == new HashSet<>(assessment.evidenceIds()).size();
    }

    private static Set<String> availableEvidence(InvestigationRoutingRequest request) {
        var available = new HashSet<String>();
        request.context().snapshot().artifacts()
                .forEach(artifact -> available.add(artifact.artifactId()));
        request.capabilityEvidence().stream()
                .filter(evidence -> request.context().snapshot().runId().equals(evidence.runId()))
                .filter(evidence -> request.context().snapshot().tenantId().equals(evidence.tenantId()))
                .forEach(evidence -> available.add(evidence.evidenceId()));
        return Set.copyOf(available);
    }

    private static boolean grantAllows(
            InvestigationRoutingRequest request,
            RoutingDecision.Target target) {
        var grants = request.context().admitted().effectiveCapabilities();
        return switch (target) {
            case INVENTORY_FOLLOW_UP -> grants.contains("read-inventory");
            case ORDER_FOLLOW_UP -> grants.contains("read-order");
            case READY_FOR_ASSESSMENT -> true;
            case MANUAL_REVIEW, INVESTIGATION_STOPPED -> false;
        };
    }

    private static RoutingDecision decision(
            InvestigationRoutingRequest request,
            RoutingDecision.Target target,
            RoutingDecision.Reason reason,
            String policyVersion,
            String advisoryClass,
            Double advisoryScore) {
        String runId = request == null || request.context() == null
                || request.context().snapshot() == null
                ? null : request.context().snapshot().runId();
        String tenantId = request == null || request.context() == null
                || request.context().snapshot() == null
                ? null : request.context().snapshot().tenantId();
        return new RoutingDecision(runId, tenantId, target, reason,
                policyVersion, advisoryClass, advisoryScore);
    }

    private static String policyVersion(RoutingPolicyContext policy) {
        return policy == null ? null : policy.policyVersion();
    }

    private static String advisoryClass(InvestigationRoutingRequest request) {
        return request == null || request.assessment() == null
                ? null : request.assessment().recommendedClass();
    }

    private static Double advisoryScore(InvestigationRoutingRequest request) {
        return request == null || request.assessment() == null
                ? null : request.assessment().supportScore();
    }
}
