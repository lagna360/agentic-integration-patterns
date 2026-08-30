package dev.agenticintegrationpatterns.orderdesk.effect;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration(proxyBeanMethods = false)
public class EffectLedgerConfiguration {
    @Bean
    @ConditionalOnMissingBean(AfterEffectStateHook.class)
    AfterEffectStateHook afterEffectStateHook() {
        return (tenantId, effectId, state, version) -> { };
    }

    @Bean
    @ConditionalOnMissingBean(TargetIdempotencyPolicy.class)
    TargetIdempotencyPolicy targetIdempotencyPolicy() {
        return new TargetIdempotencyPolicy(
                "warehouse-reservation-v1", Duration.ofHours(24));
    }

    public record TargetIdempotencyPolicy(String contractRef, Duration retention) {
        public TargetIdempotencyPolicy {
            ReserveInventoryEffect.requireText(contractRef, "contractRef", 240);
            if (retention == null || retention.isZero() || retention.isNegative()) {
                throw new IllegalArgumentException("retention must be positive");
            }
        }
    }
}
