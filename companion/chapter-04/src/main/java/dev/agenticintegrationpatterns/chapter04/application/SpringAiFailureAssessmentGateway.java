package dev.agenticintegrationpatterns.chapter04.application;

import dev.agenticintegrationpatterns.chapter04.model.AssessmentProvenance;
import dev.agenticintegrationpatterns.chapter04.model.AssessmentRequest;
import dev.agenticintegrationpatterns.chapter04.model.FailureAssessment;
import dev.agenticintegrationpatterns.chapter04.model.GatewayAssessment;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("openai")
public class SpringAiFailureAssessmentGateway implements FailureAssessmentGateway {
    private static final String INSTRUCTION_VERSION = "chapter-04-v1";

    private final ChatClient chatClient;
    private final String model;

    public SpringAiFailureAssessmentGateway(
            ChatClient.Builder builder,
            @Value("${spring.ai.openai.chat.options.model}") String model) {
        this.chatClient = builder.build();
        this.model = model;
    }

    @Override
    public GatewayAssessment assess(AssessmentRequest request) {
        try {
            FailureAssessment assessment = chatClient.prompt()
                    .system("""
                            You assess inventory-shortfall evidence. Return only the requested structure.
                            Never authorize an action. Cite only supplied evidence references.
                            If evidence is missing or ambiguous, request manual review.
                            """)
                    .user("Assess this case and context: " + request)
                    .call()
                    .entity(FailureAssessment.class, spec -> spec.useProviderStructuredOutput());
            return new GatewayAssessment(
                    assessment,
                    new AssessmentProvenance("openai", model, INSTRUCTION_VERSION));
        } catch (RuntimeException failure) {
            throw new AssessmentGatewayException("Assessment provider unavailable", failure);
        }
    }
}
