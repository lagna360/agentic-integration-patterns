package dev.agenticintegrationpatterns.orderdesk.transport.kafka;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

@TestConfiguration(proxyBeanMethods = false)
class FixedKafkaClockTestConfiguration {
    @Bean
    @Primary
    Clock fixedKafkaClock() {
        return Clock.fixed(Instant.parse("2026-08-24T06:14:00Z"), ZoneOffset.UTC);
    }
}
