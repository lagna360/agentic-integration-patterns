package dev.agenticintegrationpatterns.orderdesk.peer;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class FixtureProtectedPeerRegistry {
    private final Map<String, ProtectedPeerRegistration> registrations =
            new ConcurrentHashMap<>();

    public Optional<ProtectedPeerRegistration> find(String peerRef) {
        return Optional.ofNullable(registrations.get(peerRef));
    }

    public void put(ProtectedPeerRegistration registration) {
        registrations.put(registration.peerRef(), registration);
    }

    public void reset() {
        registrations.clear();
    }
}
