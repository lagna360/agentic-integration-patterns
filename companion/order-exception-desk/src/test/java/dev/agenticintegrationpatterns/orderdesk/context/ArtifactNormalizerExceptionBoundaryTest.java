package dev.agenticintegrationpatterns.orderdesk.context;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

import static dev.agenticintegrationpatterns.orderdesk.context.ContextResolutionException.Reason.UNSUPPORTED_CONTENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactNormalizerExceptionBoundaryTest {

    @Test
    void translatesMalformedJsonEvidence() {
        assertThatThrownBy(() -> new ArtifactNormalizer(new ObjectMapper())
                .normalize(jsonArtifact("{")))
                .isInstanceOfSatisfying(ContextResolutionException.class,
                        failure -> assertThat(failure.reason()).isEqualTo(UNSUPPORTED_CONTENT));
    }

    @Test
    void doesNotMisclassifyMapperProgrammingDefectsAsMalformedEvidence() {
        ObjectMapper mapper = new ObjectMapper() {
            @Override
            public JsonNode readTree(String content) {
                throw new NullPointerException("mapper programming defect");
            }
        };

        assertThatThrownBy(() -> new ArtifactNormalizer(mapper)
                .normalize(jsonArtifact("{}")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mapper programming defect");
    }

    private static SourceArtifact jsonArtifact(String content) {
        return new SourceArtifact(
                "tenant-ca", "inventory://yyz-01/sku@1", "inventory",
                "1", null, null, "application/json", null,
                content.getBytes(StandardCharsets.UTF_8));
    }
}
