package dev.agenticintegrationpatterns.orderdesk.context;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public final class ConservativeUtf8TokenEstimator implements TokenEstimator {
    @Override
    public String version() {
        return "utf8-bytes-div-3-teaching-estimate-v1";
    }

    @Override
    public int estimate(String text) {
        int bytes = text.getBytes(StandardCharsets.UTF_8).length;
        return (bytes + 2) / 3;
    }
}
