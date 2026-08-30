package dev.agenticintegrationpatterns.orderdesk.peer;

import org.springframework.stereotype.Component;

@Component
public class FixtureProtectedRemotePeerContextProvider {
    private volatile ProtectedRemotePeerContext current;

    public ProtectedRemotePeerContext current() {
        return current;
    }

    public void useForTest(ProtectedRemotePeerContext context) {
        current = context;
    }
}
