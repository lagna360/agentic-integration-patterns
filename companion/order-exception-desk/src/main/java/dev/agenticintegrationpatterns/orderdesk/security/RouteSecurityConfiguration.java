package dev.agenticintegrationpatterns.orderdesk.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class RouteSecurityConfiguration {
    @Bean
    @ConditionalOnMissingBean(AfterSecurityDecisionHook.class)
    AfterSecurityDecisionHook afterSecurityDecisionHook() {
        return decision -> { };
    }
}
