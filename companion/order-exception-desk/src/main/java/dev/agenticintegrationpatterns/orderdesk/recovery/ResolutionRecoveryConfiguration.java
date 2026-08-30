package dev.agenticintegrationpatterns.orderdesk.recovery;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class ResolutionRecoveryConfiguration {
    @Bean
    @ConditionalOnMissingBean(AfterResolutionStateHook.class)
    AfterResolutionStateHook afterResolutionStateHook() {
        return (tenantId, planId, state, version) -> { };
    }
}
