package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaIngressConfiguration {
    @Bean
    @ConditionalOnMissingBean
    AfterInboxHook afterInboxHook() {
        return exchange -> { };
    }
}
