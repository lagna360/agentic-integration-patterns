package dev.agenticintegrationpatterns.orderdesk.retry;

public record RetrySchedulingResult(
        RetryPolicyDecision policyDecision,
        RetryScheduleReceipt scheduleReceipt) {
}
