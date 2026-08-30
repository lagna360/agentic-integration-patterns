package dev.agenticintegrationpatterns.orderdesk.retry;

public record RetryAdmissionResult(
        Disposition disposition,
        RetryAttemptPermit permit) {

    public static RetryAdmissionResult refused(Disposition disposition) {
        return new RetryAdmissionResult(disposition, null);
    }

    public enum Disposition {
        PERMITTED,
        CAPACITY_FULL,
        CIRCUIT_OPEN,
        NOT_DUE_OR_ALREADY_CLAIMED
    }
}
