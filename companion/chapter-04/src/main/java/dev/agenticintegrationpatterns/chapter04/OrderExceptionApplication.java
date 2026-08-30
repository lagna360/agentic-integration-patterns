package dev.agenticintegrationpatterns.chapter04;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "dev.agenticintegrationpatterns")
public class OrderExceptionApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderExceptionApplication.class, args);
    }
}
