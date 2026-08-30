package dev.agenticintegrationpatterns.orderdesk.approval;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

final class ApprovalFingerprints {
    private ApprovalFingerprints() {
    }

    static String subject(ApprovalSubject s) {
        return digest(List.of(
                s.tenantId(), s.runId(), s.caseId(), s.proposalEventId(), s.proposalId(),
                s.category(), s.effectId(), s.warehouseId(), s.sku(), Integer.toString(s.quantity()),
                s.effectIntentSha256(),
                s.evidenceSetRef(), s.evidenceSetSha256(), s.contextSnapshotId(),
                s.configurationRefs().toString(), Long.toString(s.incrementalShippingCostMinor()),
                s.currency(), s.evidenceValidUntil().toString(), s.proposerRef(),
                s.subjectDigestVersion(), s.riskClass().name()));
    }

    static String decision(ApprovalDecision d, TrustedApproverContext c) {
        return digest(List.of(d.decisionId(), d.requestId(), Long.toString(d.expectedVersion()),
                d.action().name(), d.reasonCode(), c.tenantId(), c.actorRef(),
                c.authenticatedChannelRef(), c.roles().stream().sorted().toList().toString()));
    }

    static String payload(ApprovalSubject s) {
        return String.join("|", s.proposalId(), s.effectId(), s.category(), s.warehouseId(),
                s.sku(), Integer.toString(s.quantity()), s.evidenceSetRef(), s.evidenceSetSha256(),
                s.effectIntentSha256(), s.contextSnapshotId(), s.configurationRefs().toString(),
                Long.toString(s.incrementalShippingCostMinor()), s.currency(),
                s.evidenceValidUntil().toString(), s.subjectDigestVersion());
    }

    private static String digest(List<String> values) {
        try {
            var md = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                md.update(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
                md.update((byte) ':');
                md.update(bytes);
                md.update((byte) ';');
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
