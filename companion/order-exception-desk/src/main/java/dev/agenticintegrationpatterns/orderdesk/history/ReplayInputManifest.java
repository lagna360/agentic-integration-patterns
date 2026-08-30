package dev.agenticintegrationpatterns.orderdesk.history;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

public record ReplayInputManifest(
        String tenantId,
        String manifestId,
        String sourceCaseId,
        String sourceRunId,
        String asOfEventId,
        String snapshotRef,
        String snapshotSha256,
        String evidenceSetRef,
        String evidenceSetSha256,
        String modelRef,
        String instructionRef,
        String toolCatalogRef,
        String policyRef,
        String configurationRef,
        String manifestSha256,
        String retentionState) {

    public String calculatedSha256() {
        List<Object> fields = new ArrayList<>();
        fields.add(tenantId);
        fields.add(manifestId);
        fields.add(sourceCaseId);
        fields.add(sourceRunId);
        fields.add(asOfEventId);
        fields.add(snapshotRef);
        fields.add(snapshotSha256);
        fields.add(evidenceSetRef);
        fields.add(evidenceSetSha256);
        fields.add(modelRef);
        fields.add(instructionRef);
        fields.add(toolCatalogRef);
        fields.add(policyRef);
        fields.add(configurationRef);
        return canonicalSha256(fields);
    }

    /** Length-prefixed UTF-8 fields keep nulls and embedded separators unambiguous. */
    static String canonicalSha256(List<?> fields) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(fields.size());
                for (Object field : fields) {
                    if (field == null) {
                        output.writeInt(-1);
                    } else {
                        byte[] encoded = String.valueOf(field).getBytes(StandardCharsets.UTF_8);
                        output.writeInt(encoded.length);
                        output.write(encoded);
                    }
                }
            }
            return sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static String sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
