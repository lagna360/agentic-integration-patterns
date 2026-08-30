package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import org.apache.camel.Exchange;

@FunctionalInterface
public interface AfterInboxHook {
    void afterInbox(Exchange exchange);
}
