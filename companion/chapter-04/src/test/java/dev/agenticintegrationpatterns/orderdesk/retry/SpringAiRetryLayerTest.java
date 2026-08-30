package dev.agenticintegrationpatterns.orderdesk.retry;

import com.sun.net.httpserver.HttpServer;
import dev.agenticintegrationpatterns.chapter04.OrderExceptionApplication;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpringAiRetryLayerTest {
    @Test
    // tag::spring-ai-zero-hidden-retries-test[]
    void springAiAndItsOpenAiClientMakeExactlyOneRequestForA503() throws Exception {
        var calls = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            byte[] body = "{\"error\":{\"message\":\"forced 503\"}}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("content-type", "application/json");
            exchange.sendResponseHeaders(503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/v1";

        try (var context = new SpringApplicationBuilder(OrderExceptionApplication.class)
                .web(WebApplicationType.NONE)
                .profiles("openai")
                .run(
                        "--spring.ai.openai.api-key=test-key",
                        "--spring.ai.openai.base-url=" + baseUrl,
                        "--spring.ai.openai.max-retries=0",
                        "--spring.ai.retry.max-attempts=0",
                        "--spring.ai.openai.chat.options.model=test-model",
                        "--spring.datasource.url=jdbc:h2:mem:retry-layer-proof;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
                        "--camel.springboot.main-run-controller=false",
                        "--orderdesk.kafka.enabled=false")) {
            var model = context.getBean(ChatModel.class);

            assertThatThrownBy(() -> model.call("one request only"))
                    .isInstanceOf(RuntimeException.class);
            assertThat(calls).hasValue(1);
        } finally {
            server.stop(0);
        }
    }
    // end::spring-ai-zero-hidden-retries-test[]
}
