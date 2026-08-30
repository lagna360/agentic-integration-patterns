package dev.agenticintegrationpatterns.orderdesk.capability;

public final class InventoryDependencyUnavailableException extends RuntimeException {
    public InventoryDependencyUnavailableException(String message) {
        super(message);
    }

    public InventoryDependencyUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
