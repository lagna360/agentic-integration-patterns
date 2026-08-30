# Reproducibility and release binding

## Required environment

- JDK 21
- Maven Wrapper 3.3.4 downloading Apache Maven 3.9.16
- Maven distribution SHA-256 `5af3b743dd8b876b5c45da33b676251e5f1687712644abb4ee519ca56e1d89ce`
- Docker only for the optional Kafka compatibility profile

The POM pins Apache Camel 4.22.0, Spring Boot 4.1.0, Spring AI 2.0.0, Apache Kafka 4.3.1, Testcontainers 2.0.5, and NetworkNT JSON Schema Validator 3.0.6.

## Deterministic verification

From a clean checkout:

```shell
./mvnw -B -ntp clean verify
```

Expected version 1 result: 279 tests, zero failures, zero errors, zero skips. The default disables the live model and Kafka consumer, so no provider key, broker, or Docker daemon is needed after dependency resolution.

## Optional Kafka verification

```shell
./mvnw -B -ntp -Pkafka-it clean verify
```

Expected additional result: two Testcontainers integration tests against `apache/kafka:4.3.1`.

## Source identity

The companion baseline is not identified by a moving default branch. Record the immutable Git commit SHA after a clean build and use that SHA when referring to the verified companion from the book's technical records. A companion-specific annotated tag may be created later if the author wants one, but no tag is required to distribute the book.

This repository contains source code only. Do not add or attach the manuscript, PDF, EPUB, cover, publishing workspace, checksum bundle, or other book-download assets. The book's distribution channel and its artifact verification procedure are separate decisions.
