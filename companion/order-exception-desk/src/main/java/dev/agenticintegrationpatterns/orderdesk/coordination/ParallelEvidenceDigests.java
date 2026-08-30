package dev.agenticintegrationpatterns.orderdesk.coordination;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

final class ParallelEvidenceDigests {
    private ParallelEvidenceDigests() {
    }

    static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", impossible);
        }
    }

    static String replyFingerprint(InvestigationReply reply) {
        StringBuilder material = new StringBuilder();
        append(material, reply.scatterId());
        append(material, reply.runId());
        append(material, reply.tenantId());
        append(material, reply.branch().name());
        append(material, reply.status().name());
        append(material, reply.completedAt());
        if (reply.finding() != null) {
            append(material, reply.finding().evidenceKey());
            append(material, reply.finding().canonicalValue());
            append(material, reply.finding().valueSha256());
            append(material, reply.finding().sourceSystem());
            append(material, reply.finding().sourceVersion());
            append(material, reply.finding().observedAt());
        }
        return sha256(material.toString());
    }

    private static void append(StringBuilder target, Object value) {
        String text = value == null ? "<null>" : value.toString();
        target.append(text.length()).append(':').append(text).append('|');
    }
}
