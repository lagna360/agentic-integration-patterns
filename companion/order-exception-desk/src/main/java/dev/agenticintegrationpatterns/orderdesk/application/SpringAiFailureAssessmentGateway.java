package dev.agenticintegrationpatterns.orderdesk.application;

import com.openai.errors.OpenAIException;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentProvenance;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import dev.agenticintegrationpatterns.orderdesk.model.FailureAssessment;
import dev.agenticintegrationpatterns.orderdesk.model.GatewayAssessment;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

@Component
@Profile("openai")
public class SpringAiFailureAssessmentGateway implements FailureAssessmentGateway {
    private static final String INSTRUCTION_VERSION = "order-exception-assessment-v1";

    private final ChatClient chatClient;
    private final String model;

    @Autowired
    public SpringAiFailureAssessmentGateway(
            ChatClient.Builder builder,
            @Value("${spring.ai.openai.chat.options.model}") String model) {
        this(builder.build(), model);
    }

    SpringAiFailureAssessmentGateway(ChatClient chatClient, String model) {
        this.chatClient = chatClient;
        this.model = model;
    }

    @Override
    public GatewayAssessment assess(AssessmentRequest request) {
        try {
            // tag::ch4-live-gateway[]
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
            // end::ch4-live-gateway[]
        } catch (OpenAIException | JacksonException failure) {
            throw new AssessmentGatewayException("Assessment provider unavailable", failure);
        }
    }
}
