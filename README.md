# Agentic Integration Patterns - companion code

This repository contains the tested companion code for *Agentic Integration Patterns: Engineering Action-Safe, Event-Driven AI with Apache Camel* by Pankaj Upreti.

The book is the primary deliverable. The code is a set of focused, executable teaching slices for Chapters 4-22, not an agent framework, production platform, or deployable reference architecture.

## Book availability

The book is not distributed from this repository. This repository contains companion source code only; it must not contain the manuscript, PDF, EPUB, cover, publishing workspace, or book-download assets. The author has not yet selected the book's public distribution channel.

## Baseline

- Java 21
- Maven 3.9.16 through the checked-in wrapper
- Apache Camel 4.22.0
- Spring Boot 4.1.0
- Spring AI 2.0.0
- Apache Kafka client and optional broker 4.3.1
- Testcontainers 2.0.5
- JSON Schema Draft 2020-12 with NetworkNT validator 3.0.6

## Verify the deterministic companion

No model-provider key, broker, or Docker daemon is required after Maven dependencies are cached:

```shell
./mvnw -B -ntp clean verify
```

The version 1 baseline contains 292 deterministic tests. They prove bounded application behavior under the included fixtures; they do not certify a production deployment, provider, target system, identity platform, or infrastructure configuration.

## Verify the Kafka compatibility slice

With Docker available:

```shell
./mvnw -B -ntp -Pkafka-it clean verify
```

The optional profile adds two Testcontainers integration tests against `apache/kafka:4.3.1`.

## Optional live model profile

The default build is keyless. A separately activated `openai` profile demonstrates the provider boundary:

```shell
export OPENAI_API_KEY=replace-with-a-real-secret
export OPENAI_MODEL=gpt-5-mini
./mvnw -pl companion/order-exception-desk spring-boot:run \
  -Dspring-boot.run.profiles=openai
```

Never commit credentials. The live profile is outside routine release verification and is not a provider-quality claim.

## Repository map

- `companion/order-exception-desk/src/main/` - the incremental Order Exception Desk implementation used across Chapters 4-22
- `companion/order-exception-desk/src/test/` - deterministic and forced-failure evidence
- `companion/order-exception-desk/README.md` - detailed scope and deliberately missing production properties
- `REPRODUCIBILITY.md` - exact build and release-binding procedure
- `ERRATA.md` - public companion-code correction ledger
- `SECURITY.md` - safe reporting guidance and teaching-code scope

## Licenses

| Material | License |
|---|---|
| Original companion source, tests, and build configuration | MIT License (`LICENSE`) |
| Maven Wrapper scripts | Apache License 2.0 (`LICENSES/Apache-2.0.txt` and `THIRD_PARTY_NOTICES.md`) |
| Downloaded dependencies and container images | Their respective upstream licenses; not redistributed in the source package |

Apache, Apache Camel, Camel, Apache Kafka, and Kafka are either registered trademarks or trademarks of The Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied.

## Errata and issues

Use [`ERRATA.md`](ERRATA.md) for confirmed companion-code corrections and GitHub Issues for non-sensitive code reports. Follow [`SECURITY.md`](SECURITY.md) for suspected security issues; do not post secrets, credentials, or exploit details in a public issue. Book distribution and book-specific reporting channels will be documented separately when the author chooses them.
