package dev.agenticintegrationpatterns.orderdesk.context;

import java.util.Arrays;

record PreparedContextArtifact(ContextArtifact artifact, byte[] sourceBytes) {
    PreparedContextArtifact {
        sourceBytes = Arrays.copyOf(sourceBytes, sourceBytes.length);
    }

    @Override
    public byte[] sourceBytes() {
        return Arrays.copyOf(sourceBytes, sourceBytes.length);
    }
}
