package dev.agenticintegrationpatterns.orderdesk.retry;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;

@Configuration(proxyBeanMethods = false)
public class RetryControlConfiguration {
    @Bean
    @ConditionalOnMissingBean(EqualJitterBackoff.class)
    EqualJitterBackoff equalJitterBackoff() {
        return new EqualJitterBackoff();
    }

    @Bean
    EstablishedRetryPolicy establishedRetryPolicy(Clock clock, EqualJitterBackoff backoff) {
        return new EstablishedRetryPolicy(clock, backoff);
    }

    @Bean
    @ConditionalOnMissingBean(RetryCapacityGate.class)
    RetryCapacityGate retryCapacityGate() {
        return new SemaphoreRetryCapacityGate(8);
    }

    @Bean
    @ConditionalOnMissingBean(CircuitBreaker.class)
    CircuitBreaker retryCircuitBreaker() {
        var config = CircuitBreakerConfig.custom()
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build();
        return CircuitBreaker.of("orderdesk-retry-attempt", config);
    }
}
