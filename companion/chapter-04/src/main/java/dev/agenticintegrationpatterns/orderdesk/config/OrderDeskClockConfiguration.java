package dev.agenticintegrationpatterns.orderdesk.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
public class OrderDeskClockConfiguration {
    @Bean
    @ConditionalOnMissingBean(Clock.class)
    Clock orderDeskClock() {
        return Clock.systemUTC();
    }
}
