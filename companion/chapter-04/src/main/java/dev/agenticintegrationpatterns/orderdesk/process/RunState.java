package dev.agenticintegrationpatterns.orderdesk.process;

public enum RunState {
    WAITING_FOR_EVIDENCE,
    READY_FOR_ASSESSMENT,
    REVIEW_REQUIRED,
    PAUSED,
    COMPLETED,
    STOPPED
}
