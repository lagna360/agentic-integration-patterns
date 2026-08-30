package dev.agenticintegrationpatterns.orderdesk.security;

import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Set;

/** Test transport adapter. Production replaces this with validated channel/broker context. */
@Component
public class FixtureProtectedContextProvider {
    private final Clock clock;
    private volatile ProtectedRouteContext current;

    public FixtureProtectedContextProvider(Clock clock) {
        this.clock = clock;
        reset();
    }

    public SecurityAdmission attach(SecuredRouteMessage message) {
        return new SecurityAdmission(message, current);
    }

    public void reset() {
        var now = clock.instant();
        current = new ProtectedRouteContext(
                "workload:effect-gateway-consumer", "service:order-desk-effect-gateway",
                null, Set.of(ProtectedRouteState.TENANT), JdbcRouteSecurityGate.EFFECT_AUDIENCE,
                "mTLS", "svid-key-41", Set.of("EFFECT_GATEWAY"), "WORKLOAD_MTLS",
                now.minusSeconds(10), now.plusSeconds(60));
    }

    public void useForTest(ProtectedRouteContext context) {
        current = context;
    }
}
