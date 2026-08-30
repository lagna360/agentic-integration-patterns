package dev.agenticintegrationpatterns.orderdesk.coordination;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;

import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationReply.Status.SUCCEEDED;
import static dev.agenticintegrationpatterns.orderdesk.coordination.InvestigationReply.Status.UNAVAILABLE;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.Completion.*;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.Disposition.*;
import static dev.agenticintegrationpatterns.orderdesk.coordination.ParallelEvidenceSet.Reason.*;

@Component
public final class ParallelEvidenceReducer {

    public ParallelEvidenceAccumulator start(ParallelInvestigationPlan plan) {
        validatePlan(plan);
        return new ParallelEvidenceAccumulator(
                plan, new EnumMap<>(InvestigationBranch.class), List.of(), 0, null, null);
    }

    // tag::merge-reply[]
    public MergeResult merge(
            ParallelEvidenceAccumulator current,
            InvestigationReply incoming,
            Instant receivedAt) {
        validateReply(current.plan(), incoming, receivedAt);
        if (current.closed() || !receivedAt.isBefore(current.plan().deadlineAt())) {
            return new MergeResult(current, MergeDisposition.LATE);
        }

        InvestigationReply existing = current.replies().get(incoming.branch());
        if (existing != null) {
            if (ParallelEvidenceDigests.replyFingerprint(existing).equals(
                    ParallelEvidenceDigests.replyFingerprint(incoming))) {
                return new MergeResult(copy(current, current.replies(), current.conflicts(),
                        current.duplicateCount() + 1), MergeDisposition.DUPLICATE);
            }
            var conflict = conflict(existing, incoming, "branch-result:" + incoming.branch());
            return new MergeResult(copy(current, current.replies(), append(current.conflicts(), conflict),
                    current.duplicateCount()), MergeDisposition.CONFLICT);
        }

        var conflicts = new ArrayList<>(current.conflicts());
        if (incoming.finding() != null) {
            current.replies().values().stream()
                    .filter(reply -> reply.finding() != null)
                    .filter(reply -> reply.finding().evidenceKey()
                            .equals(incoming.finding().evidenceKey()))
                    .filter(reply -> !reply.finding().valueSha256()
                            .equals(incoming.finding().valueSha256()))
                    .map(reply -> conflict(reply, incoming, incoming.finding().evidenceKey()))
                    .forEach(conflicts::add);
        }

        var replies = new EnumMap<InvestigationBranch, InvestigationReply>(InvestigationBranch.class);
        replies.putAll(current.replies());
        replies.put(incoming.branch(), incoming);
        var merged = copy(current, replies, conflicts, current.duplicateCount());
        return new MergeResult(merged,
                conflicts.size() == current.conflicts().size()
                        ? MergeDisposition.ADDED : MergeDisposition.CONFLICT);
    }
    // end::merge-reply[]

    public ParallelEvidenceSet close(
            ParallelEvidenceAccumulator current,
            Instant closedAt,
            ParallelEvidenceSet.CompletionTrigger trigger) {
        if (current.closed()) {
            closedAt = current.closedAt();
            trigger = current.completionTrigger();
        }
        var missing = EnumSet.copyOf(current.plan().expectedBranches());
        missing.removeAll(current.replies().keySet());
        var unavailable = EnumSet.noneOf(InvestigationBranch.class);
        current.replies().forEach((branch, reply) -> {
            if (reply.status() == UNAVAILABLE) {
                unavailable.add(branch);
            }
        });
        boolean requiredMissing = current.plan().requiredBranches().stream()
                .anyMatch(branch -> missing.contains(branch) || unavailable.contains(branch));

        ParallelEvidenceSet.Completion completion;
        ParallelEvidenceSet.Disposition disposition;
        ParallelEvidenceSet.Reason reason;
        if (!current.conflicts().isEmpty()) {
            completion = CONFLICTED;
            disposition = MANUAL_REVIEW;
            reason = EVIDENCE_CONFLICT;
        } else if (missing.isEmpty() && unavailable.isEmpty()) {
            completion = COMPLETE;
            disposition = EVIDENCE_READY;
            reason = ALL_EXPECTED_EVIDENCE;
        } else if (requiredMissing) {
            completion = PARTIAL;
            disposition = MANUAL_REVIEW;
            reason = REQUIRED_EVIDENCE_MISSING;
        } else {
            completion = PARTIAL;
            disposition = current.plan().allowPartialWhenRequiredComplete()
                    ? EVIDENCE_READY : MANUAL_REVIEW;
            reason = OPTIONAL_EVIDENCE_MISSING;
        }

        var replies = current.replies().values().stream()
                .sorted(Comparator.comparing(reply -> reply.branch().name()))
                .toList();
        return new ParallelEvidenceSet(
                current.plan().scatterId(), current.plan().runId(), current.plan().tenantId(),
                current.plan().planVersion(), completion, disposition, reason, trigger,
                replies, missing, unavailable, current.conflicts(), current.duplicateCount(), closedAt);
    }

    private static void validatePlan(ParallelInvestigationPlan plan) {
        if (plan == null || blank(plan.scatterId()) || blank(plan.planVersion())
                || blank(plan.runId()) || blank(plan.tenantId()) || plan.deadlineAt() == null
                || plan.expectedBranches() == null || plan.expectedBranches().isEmpty()
                || plan.requiredBranches() == null
                || !plan.expectedBranches().containsAll(plan.requiredBranches())) {
            throw new IllegalArgumentException("A bounded parallel investigation plan is required");
        }
    }

    private static void validateReply(
            ParallelInvestigationPlan plan,
            InvestigationReply reply,
            Instant receivedAt) {
        if (reply == null || blank(reply.replyId()) || reply.branch() == null
                || reply.status() == null || reply.completedAt() == null
                || reply.completedAt().isAfter(receivedAt)
                || !plan.scatterId().equals(reply.scatterId())
                || !plan.runId().equals(reply.runId())
                || !plan.tenantId().equals(reply.tenantId())
                || !plan.expectedBranches().contains(reply.branch())) {
            throw new InvalidInvestigationReplyException(
                    "Reply identity, membership, or time is invalid for this scatter");
        }
        if (reply.status() == SUCCEEDED) {
            var finding = reply.finding();
            if (finding == null || blank(finding.evidenceKey())
                    || blank(finding.canonicalValue()) || blank(finding.valueSha256())
                    || blank(finding.sourceSystem()) || blank(finding.sourceVersion())
                    || finding.observedAt() == null || finding.observedAt().isAfter(receivedAt)
                    || !ParallelEvidenceDigests.sha256(finding.canonicalValue())
                            .equals(finding.valueSha256())) {
                throw new InvalidInvestigationReplyException(
                        "Successful reply evidence is malformed or has an invalid digest");
            }
        } else if (reply.finding() != null) {
            throw new InvalidInvestigationReplyException(
                    "Unavailable replies cannot carry admitted evidence");
        }
    }

    private static EvidenceConflict conflict(
            InvestigationReply first,
            InvestigationReply second,
            String evidenceKey) {
        String firstHash = first.finding() == null
                ? ParallelEvidenceDigests.replyFingerprint(first) : first.finding().valueSha256();
        String secondHash = second.finding() == null
                ? ParallelEvidenceDigests.replyFingerprint(second) : second.finding().valueSha256();
        return new EvidenceConflict(first.branch(), second.branch(),
                evidenceKey, firstHash, secondHash);
    }

    private static ParallelEvidenceAccumulator copy(
            ParallelEvidenceAccumulator current,
            java.util.Map<InvestigationBranch, InvestigationReply> replies,
            List<EvidenceConflict> conflicts,
            int duplicates) {
        return new ParallelEvidenceAccumulator(
                current.plan(), replies, conflicts, duplicates,
                current.closedAt(), current.completionTrigger());
    }

    private static List<EvidenceConflict> append(
            List<EvidenceConflict> existing,
            EvidenceConflict next) {
        var result = new ArrayList<>(existing);
        result.add(next);
        return List.copyOf(result);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    public enum MergeDisposition {
        ADDED,
        DUPLICATE,
        CONFLICT,
        LATE
    }

    public record MergeResult(
            ParallelEvidenceAccumulator accumulator,
            MergeDisposition disposition) {
    }
}
