package dev.agenticintegrationpatterns.orderdesk.context;

import tools.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static dev.agenticintegrationpatterns.orderdesk.context.ContextResolutionException.Reason.UNSUPPORTED_CONTENT;

@Component
public final class ArtifactNormalizer {
    private static final Set<String> TEXT_TYPES = Set.of(
            "text/plain", "application/json");

    private final ObjectMapper mapper;

    public ArtifactNormalizer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String normalize(SourceArtifact artifact) {
        String mediaType = artifact.contentType() == null
                ? "" : artifact.contentType().split(";", 2)[0].strip().toLowerCase();
        if (!TEXT_TYPES.contains(mediaType)) {
            throw new ContextResolutionException(
                    UNSUPPORTED_CONTENT, artifact.reference(), "Content type is not allowlisted");
        }

        String text = strictUtf8(artifact.reference(), artifact.content());
        if ("application/json".equals(mediaType)) {
            try {
                return mapper.writeValueAsString(mapper.readTree(text));
            } catch (Exception exception) {
                throw new ContextResolutionException(
                        UNSUPPORTED_CONTENT, artifact.reference(), "JSON evidence is malformed");
            }
        }
        return text.replace("\r\n", "\n").replace('\r', '\n');
    }

    private static String strictUtf8(String reference, byte[] content) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(content))
                    .toString();
        } catch (CharacterCodingException exception) {
            throw new ContextResolutionException(
                    UNSUPPORTED_CONTENT, reference, "Evidence is not valid UTF-8");
        }
    }
}
