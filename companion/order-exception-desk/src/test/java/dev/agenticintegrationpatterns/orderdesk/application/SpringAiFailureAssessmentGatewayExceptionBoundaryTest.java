package dev.agenticintegrationpatterns.orderdesk.application;

import com.openai.errors.OpenAIIoException;
import dev.agenticintegrationpatterns.orderdesk.model.AssessmentRequest;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import tools.jackson.core.exc.StreamReadException;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpringAiFailureAssessmentGatewayExceptionBoundaryTest {
    private static final AssessmentRequest REQUEST = new AssessmentRequest(null, null);

    @Test
    void translatesProviderFailures() {
        var providerFailure = new OpenAIIoException("provider connection failed");
        var gateway = gatewayThrowingAtCall(providerFailure);

        assertThatThrownBy(() -> gateway.assess(REQUEST))
                .isInstanceOf(AssessmentGatewayException.class)
                .hasCause(providerFailure);
    }

    @Test
    void translatesMalformedStructuredOutput() {
        var malformedOutput = new StreamReadException("malformed structured output");
        var gateway = gatewayThrowingAtEntityConversion(malformedOutput);

        assertThatThrownBy(() -> gateway.assess(REQUEST))
                .isInstanceOf(AssessmentGatewayException.class)
                .hasCause(malformedOutput);
    }

    @Test
    void doesNotMisclassifyProgrammingDefectsAsProviderFailures() {
        var programmingDefect = new NullPointerException("gateway programming defect");
        var gateway = gatewayThrowingAtEntityConversion(programmingDefect);

        assertThatThrownBy(() -> gateway.assess(REQUEST))
                .isSameAs(programmingDefect);
    }

    private static SpringAiFailureAssessmentGateway gatewayThrowingAtCall(RuntimeException failure) {
        return gateway(failure, null);
    }

    private static SpringAiFailureAssessmentGateway gatewayThrowingAtEntityConversion(
            RuntimeException failure) {
        return gateway(null, failure);
    }

    private static SpringAiFailureAssessmentGateway gateway(
            RuntimeException callFailure,
            RuntimeException entityFailure) {
        ChatClient.CallResponseSpec response = (ChatClient.CallResponseSpec) Proxy.newProxyInstance(
                ChatClient.CallResponseSpec.class.getClassLoader(),
                new Class<?>[]{ChatClient.CallResponseSpec.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("entity")) {
                        throw entityFailure;
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
        ChatClient.ChatClientRequestSpec request =
                (ChatClient.ChatClientRequestSpec) Proxy.newProxyInstance(
                        ChatClient.ChatClientRequestSpec.class.getClassLoader(),
                        new Class<?>[]{ChatClient.ChatClientRequestSpec.class},
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("call")) {
                                if (callFailure != null) {
                                    throw callFailure;
                                }
                                return response;
                            }
                            if (method.getReturnType().equals(ChatClient.ChatClientRequestSpec.class)) {
                                return proxy;
                            }
                            throw new UnsupportedOperationException(method.toString());
                        });
        ChatClient client = (ChatClient) Proxy.newProxyInstance(
                ChatClient.class.getClassLoader(),
                new Class<?>[]{ChatClient.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("prompt") && method.getParameterCount() == 0) {
                        return request;
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
        return new SpringAiFailureAssessmentGateway(client, "test-model");
    }
}
