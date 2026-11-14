# connect-reliability-lab

A hands-on course where you learn every part of the **Connect Service** (Postgres CDC →
Kafka → ClickHouse) and then build each upgrade from the
[Connect Service Reliability Plan](https://claude.ai/artifact/6ciuKZyrrzjC4JBtmsx3Rs) on your
own laptop.

Backend only. No frontend.

**Everything lives in [ROADMAP.md](./ROADMAP.md).** This file is just the map.

> **New here?** Read [The Connect Service in 5 minutes](./ROADMAP.md#the-connect-service-in-5-minutes)
> first. Then start at [Phase 0](./ROADMAP.md#phase-0--environment--tooling).

## How a lesson works

Every concept follows the same loop (same as `../temporal-poc`):

1. **Explain** — short, plain English, one small example.
2. **How we use it** — where it lives in our real code (`cdc-etl-connect`, `poc-debezium`).
3. **Check** — one question at a time. You answer before we move on.
4. **Build** — you type and run a small practical piece.
5. **Break it** — kill something on purpose and watch it recover.
6. **Record** — a short entry in [NOTES.md](./NOTES.md).

## Finding your way around

| If you want... | Go to |
|---|---|
| The whole system in one page | [The Connect Service in 5 minutes](./ROADMAP.md#the-connect-service-in-5-minutes) |
| Postgres WAL, slots, publications | [Phase 2](./ROADMAP.md#phase-2--postgres-wal-slots-and-publications) |
| Kafka basics and 3-broker safety | [Phase 3](./ROADMAP.md#phase-3--kafka-basics), [Phase 4](./ROADMAP.md#phase-4--kafka-durability-3-brokers-3-zones) |
| Schema Registry and Avro | [Phase 5](./ROADMAP.md#phase-5--schema-registry-and-avro) |
| Kafka Connect, Debezium, ClickHouse sink | [Phases 6–8](./ROADMAP.md#phase-6--kafka-connect) |
| Kubernetes and the move off the VM | [Phases 11–13](./ROADMAP.md#phase-11--kubernetes-basics-on-kind) |
| Metadata DB HA and app hardening | [Phase 14](./ROADMAP.md#phase-14--metadata-db-cloud-sql-ha--app-hardening) |
| Watchdog and alerts that need no DB | [Phase 15](./ROADMAP.md#phase-15--streaming-without-the-metadata-db) |
| Temporal, extractor Jobs, more writers | [Phases 16–18](./ROADMAP.md#phase-16--temporal-for-jobs) |
| Things I tested on this Mac, with results | [Appendix B](./ROADMAP.md#appendix-b--verified-on-this-mac) |
| "Why is this broken?" | [Appendix E](./ROADMAP.md#appendix-e--troubleshooting) |
| Your progress | [Appendix F](./ROADMAP.md#appendix-f--progress-tracker) |

## Stack (same versions as production where possible)

Kafka 4.1.1 (KRaft, 3 brokers) · Confluent Schema Registry 8.3.0 · Kafka Connect
(`cp-kafka-connect-base` 8.3.0) · Debezium Postgres 3.6.0.Final · ClickHouse sink 1.4.0 ·
ClickHouse 26.8 · Postgres 17 · kind v0.30.0 (Kubernetes v1.34) · Go 1.27 · Temporal dev server
(Docker, `temporalio/temporal:1.8.2`).

Every phase has a hands-on part that runs in Docker (kind = Kubernetes inside Docker).
