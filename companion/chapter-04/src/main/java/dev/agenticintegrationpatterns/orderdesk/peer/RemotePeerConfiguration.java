package dev.agenticintegrationpatterns.orderdesk.peer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class RemotePeerConfiguration {
    @Bean
    @ConditionalOnMissingBean(AfterRemotePeerTaskReadHook.class)
    AfterRemotePeerTaskReadHook afterRemotePeerTaskReadHook() {
        return (operation, tenantId, remoteWorkId, version) -> { };
    }
}
