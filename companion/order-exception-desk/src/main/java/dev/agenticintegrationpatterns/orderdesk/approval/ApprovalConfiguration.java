package dev.agenticintegrationpatterns.orderdesk.approval;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class ApprovalConfiguration {
    @Bean
    @ConditionalOnMissingBean(AfterApprovalStateHook.class)
    AfterApprovalStateHook afterApprovalStateHook() {
        return (tenantId, requestId, state, version) -> { };
    }
}
