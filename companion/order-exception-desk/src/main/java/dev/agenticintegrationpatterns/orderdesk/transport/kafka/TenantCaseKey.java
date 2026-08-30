package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import dev.agenticintegrationpatterns.orderdesk.work.InvestigateOrderException;

public final class TenantCaseKey {
    private TenantCaseKey() {}

    // tag::tenant-case-key[]
    public static String from(InvestigateOrderException command) {
        return "v1|" + segment(command.tenantId()) + "|" + segment(command.caseId());
    }

    public static void requireMatch(String actual, InvestigateOrderException command) {
        var expected = from(command);
        if (!expected.equals(actual)) {
            throw new InvalidKafkaRecordException(
                    "Kafka key does not match the command tenant and case");
        }
    }
    // end::tenant-case-key[]

    private static String segment(String value) {
        if (value == null || value.isBlank() || value.contains("|")) {
            throw new InvalidKafkaRecordException("Kafka key segment is invalid");
        }
        return value;
    }
}
