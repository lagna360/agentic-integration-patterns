package dev.agenticintegrationpatterns.orderdesk.context;

import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public final class DemoEmailRedactor implements EvidenceRedactor {
    private static final Pattern EMAIL = Pattern.compile(
            "(?i)(?<![A-Z0-9._%+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}(?![A-Z0-9._%+-])");

    @Override
    public String redact(String normalizedText) {
        return EMAIL.matcher(normalizedText).replaceAll("[REDACTED_EMAIL]");
    }
}
