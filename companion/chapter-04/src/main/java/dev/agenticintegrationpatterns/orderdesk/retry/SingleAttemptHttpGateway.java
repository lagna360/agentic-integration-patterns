package dev.agenticintegrationpatterns.orderdesk.retry;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** One HTTP request only; retry belongs to the durable application policy. */
public final class SingleAttemptHttpGateway {
    private final HttpClient client;

    public SingleAttemptHttpGateway(Duration connectTimeout) {
        client = HttpClient.newBuilder().connectTimeout(connectTimeout)
                .followRedirects(HttpClient.Redirect.NEVER).build();
    }

    public HttpResponse<String> post(URI endpoint, String json, Duration requestTimeout)
            throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(endpoint)
                .timeout(requestTimeout)
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
