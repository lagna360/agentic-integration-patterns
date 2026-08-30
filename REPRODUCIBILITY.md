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

## Release identity

The source release is not identified by the moving default branch. The release operator must:

1. Build and test a clean private candidate commit.
2. Record that immutable commit SHA in the book's release register and artifact manifest.
3. Rebuild and close the PDF, EPUB, source archive, and checksums against that exact SHA.
4. Create the annotated tag `book-v1.0.0` only after closure.
5. Verify `git rev-parse book-v1.0.0^{commit}` equals the recorded SHA and that unauthenticated `git ls-remote` exposes the same tag.
6. Download the published release assets independently and compare them with `SHA256SUMS`.

The final tag, SHA, public visibility, and downloaded-asset checks remain release-time checks and are intentionally absent from this private staging package.
