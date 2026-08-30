package dev.agenticintegrationpatterns.orderdesk.process;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;

@Configuration
public class ProcessManagerConfiguration {
    @Bean
    @ConditionalOnMissingBean(AfterProcessStateHook.class)
    AfterProcessStateHook afterProcessStateHook() {
        return (tenantId, runId, version) -> { };
    }
}
