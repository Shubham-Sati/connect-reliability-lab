# Connect Service Reliability — Zero to Advanced Roadmap

> A hands-on, build-everything course. You learn each part of our Connect Service, then you
> build each upgrade from the reliability plan on your laptop and break it on purpose.
> Backend only — no frontend.

| | |
|---|---|
| **What we copy** | Our real pipeline: Postgres → Debezium → Kafka → ClickHouse sink → ClickHouse |
| **Real code** | `~/Desktop/devs/zoop/poc-s3/cdc-etl-connect` (Go API) and `poc-debezium` (Connect infra) |
| **Plan we build** | [Connect Service Reliability Plan](https://claude.ai/artifact/6ciuKZyrrzjC4JBtmsx3Rs) |
| **Local runtime** | Docker Desktop (16 GB given to Docker is enough) + kind (Kubernetes in Docker) |
| **Language** | Go 1.27 for every tool we write |
| **Effort** | ~60–80 focused hours, 20 phases + 2 electives |

---

## Start here — pick your path

| You are… | Start at |
|---|---|
| **New to all of this** | [The Connect Service in 5 minutes](#the-connect-service-in-5-minutes), then Phase 0 and straight through |
| **Know Kafka and Postgres well** | Read the Checkpoints of Phases 2–8. If you can answer them, jump to [Part II](#part-ii--reliability-upgrades) |
| **Looking something up** | [Appendix A](#appendix-a--glossary) (words) · [Appendix D](#appendix-d--command-cheat-sheet) (commands) · [Appendix E](#appendix-e--troubleshooting) (errors) |

**Conventions**

- **Build:** code or config you write. **Break it:** a deliberate failure. **Checkpoint:** a
  self-test; if you can't answer from memory, re-read before moving on.
- ✅ **Verified** means it was actually run on this Mac on 2026-11-14 (see
  [Appendix B](#appendix-b--verified-on-this-mac)). Anything else is "expected" until *you*
  run it.
- `§N.N` always means a section in this file.
- 🏭 **In our code** boxes point to the real file in `cdc-etl-connect` or `poc-debezium`.

---

## The Connect Service in 5 minutes

*If you read nothing else, read this.*

**The job.** A team wants their Postgres table `orders` copied into ClickHouse, and kept in
sync forever — every insert, update and delete, within seconds. They also want data from
Google Ads, Facebook Ads, GCS files and Kafka. They click a few buttons; our platform does the
rest.

**The path of one change:**

```
UPDATE orders SET status='paid' WHERE id=42;
        │
        ▼
Postgres WAL (its diary of changes)
        │  replication slot = bookmark Postgres keeps for us
        ▼
Debezium (source connector)  ──schema──▶ Schema Registry (stores the row's shape, gives id 17)
        │  writes a small Avro message: key {id:42}, value {…,status:'paid', __op:'u', __ts_ms:…}
        ▼
Kafka topic  <prefix>.public.orders   (3 brokers, 3 copies of every message)
        │
        ▼
ClickHouse sink (sink connector) ── batches of thousands ──▶ ClickHouse table
                                                             ReplacingMergeTree(__ts_ms)
                                                             keeps the newest row per id
```

**Kafka Connect** is the program that runs Debezium and the sink. It saves where each one is
(its *bookmark*) inside Kafka, so any Connect worker can continue the job if another dies.

**Our Go API** (`cdc-etl-connect`) creates, pauses and deletes those connectors through
Connect's REST API, and keeps its own state (syncs, alerts, jobs) in a **metadata DB**
(Cloud SQL Postgres).

**Why no data is lost**, in four steps:

1. Postgres keeps every change in the WAL until Debezium says "Kafka has it".
2. Kafka keeps 3 copies, and only says "got it" when at least 2 have it.
3. The sink moves its bookmark only after ClickHouse accepted the batch.
4. If anything is sent twice, ClickHouse keeps one row per id (newest wins).

**What the reliability plan fixes** — today there are single points of failure:

| # | Weak spot | Upgrade (phase) |
|---|---|---|
| 1 | Connect + Schema Registry run on **one VM** | Move to Kubernetes, 3 Connect + 2 Schema Registry (Phases 11–13) |
| 2 | Connect knows **one Kafka address** | List 3 addresses + back up Connect's bookmarks and schemas (Phase 10) |
| — | Metadata DB | Already Cloud SQL HA + PITR. We add app hardening (Phase 14) |
| 3b | A failed connector needs the metadata DB to be restarted | Watchdog + alerts that need no DB (Phase 15) |
| 4–6 | Jobs, extractor, writers | Temporal, Kubernetes Jobs, more sink tasks (Phases 16–18) |

Every phase below is one piece of this picture.

---

## Table of Contents

- [How to use this roadmap](#how-to-use-this-roadmap)
- [Repository layout](#repository-layout)
- **Part I — Foundations (the data path)**
  - [Phase 0 — Environment & tooling](#phase-0--environment--tooling)
  - [Phase 1 — The big picture](#phase-1--the-big-picture)
  - [Phase 2 — Postgres WAL, slots and publications](#phase-2--postgres-wal-slots-and-publications)
  - [Phase 3 — Kafka basics](#phase-3--kafka-basics)
  - [Phase 4 — Kafka durability: 3 brokers, 3 zones](#phase-4--kafka-durability-3-brokers-3-zones)
  - [Phase 5 — Schema Registry and Avro](#phase-5--schema-registry-and-avro)
  - [Phase 6 — Kafka Connect](#phase-6--kafka-connect)
  - [Phase 7 — Debezium Postgres source](#phase-7--debezium-postgres-source)
  - [Phase 8 — ClickHouse and the sink](#phase-8--clickhouse-and-the-sink)
- **Part II — Reliability upgrades**
  - [Phase 9 — Failure thinking and baseline drills](#phase-9--failure-thinking-and-baseline-drills)
  - [Phase 10 — Kafka safety net: addresses, retention, backups](#phase-10--kafka-safety-net-addresses-retention-backups)
  - [Phase 11 — Kubernetes basics on kind](#phase-11--kubernetes-basics-on-kind)
  - [Phase 12 — Connect and Schema Registry on Kubernetes](#phase-12--connect-and-schema-registry-on-kubernetes)
  - [Phase 13 — Moving safely: VM → Kubernetes](#phase-13--moving-safely-vm--kubernetes)
  - [Phase 14 — Metadata DB: Cloud SQL HA + app hardening](#phase-14--metadata-db-cloud-sql-ha--app-hardening)
  - [Phase 15 — Streaming without the metadata DB](#phase-15--streaming-without-the-metadata-db)
  - [Phase 16 — Temporal for jobs](#phase-16--temporal-for-jobs)
  - [Phase 17 — Extractor as Kubernetes Jobs](#phase-17--extractor-as-kubernetes-jobs)
  - [Phase 18 — Faster writers and bad-table isolation](#phase-18--faster-writers-and-bad-table-isolation)
  - [Phase 19 — Capstone: game day](#phase-19--capstone-game-day)
- **Electives**
  - [E1 — MirrorMaker 2 standby cluster](#e1--mirrormaker-2-standby-cluster)
  - [E2 — When the *source* Postgres fails over](#e2--when-the-source-postgres-fails-over)
- **Appendices**
  - [A — Glossary](#appendix-a--glossary) · [B — Verified on this Mac](#appendix-b--verified-on-this-mac) ·
    [C — Ports & URLs](#appendix-c--ports--urls) · [D — Command cheat sheet](#appendix-d--command-cheat-sheet) ·
    [E — Troubleshooting](#appendix-e--troubleshooting) · [F — Progress tracker](#appendix-f--progress-tracker) ·
    [G — Plan page → phase map](#appendix-g--plan-page--phase-map) · [H — Real code map](#appendix-h--real-code-map) ·
    [I — Sources](#appendix-i--sources)

---

## How to use this roadmap

Each **module** has the same shape:

1. **Goal** — what you can do afterwards.
2. **Concepts** — what it is, why it matters, a tiny example.
3. **🏭 In our code** — where it lives in the real Connect Service.
4. **Build** — what you type, with file paths.
5. **Break it** — kill, stop or corrupt something on purpose.
6. **Checkpoint** — questions you must answer without looking.

### Required learning flow — every concept

Teach in short exchanges, one scenario and one question at a time.

1. **Explain** the concept in plain English (what it is, why it matters, one tiny example).
   Cite the section (`§4.3`).
2. **Show how the Connect Service uses it** — the real file, setting, or behaviour.
3. **Ask one check question** — "what happens if…" or "why…", never "what is the name of…".
   Wrong or partial → re-explain shorter with a new example, ask again.
4. **Build the practical piece** — the learner types and runs it.
5. **Break it on purpose** and inspect the result (status, counts, logs, lag). Compare
   expected vs observed.
6. **Apply it to a second scenario** — where else this idea helps, and where it's a poor fit.
7. **Record** in `NOTES.md`: section, goal, what you built, failure tried, expected vs
   observed, what the platform still must handle, status (**conceptual / built / verified**).

**Scenario bank** — reuse across phases:

| Scenario | Concepts | Failure to explore |
|---|---|---|
| `orders` table synced to ClickHouse | WAL, slots, Debezium, sink, ReplacingMergeTree | Connect dies mid-stream; rows sent twice |
| Busy shared Postgres, quiet `orders` table | Heartbeats, slot lag | WAL fills the disk while nothing changes in `orders` |
| Kafka broker / zone dies | RF, ISR, acks, racks, bootstrap list | 1 broker down vs 2 brokers down |
| Bad row (e.g. text where a number is expected) | `errors.tolerance`, DLQ, sink isolation | One table blocks every other table in the sink |
| Moving Connect from the VM to GKE | Groups, rebalancing, advertised host | Worker crashes during the move |
| Metadata DB fails over | Retries, `/ready`, watchdog | A connector fails *while* the DB is down |
| 2-hour GCS extraction | Temporal, Kubernetes Jobs, heartbeats | Worker pod killed halfway |

**Rules of engagement**

- Keep three terminals open: `docker compose logs -f`, a shell, and a "watch" window
  (`watch -n2 make status`).
- **Never trust "RUNNING" alone.** Always also check row counts or lag (you will see why in
  Phase 9).
- Commit after each module: `git commit -m "m7.3: debezium heartbeat"`.
- Write `NOTES.md` per phase — what surprised you, what you got wrong.

---

## Repository layout

Build toward this. Create folders as you reach them.

```
connect-reliability-lab/
├── ROADMAP.md / NOTES.md / README.md
├── Makefile                     ← up, down, status, counts, psql, ch, kafka helpers
├── p01-by-hand/                 ← Phase 1 warm-up: Postgres + 1 Kafka + ClickHouse, you are the pipeline
├── docker-compose.yml           ← Kafka x3, Schema Registry x2, Connect "VM", Postgres x2, ClickHouse,
│                                  Prometheus, Temporal (added phase by phase)
├── connect/Dockerfile           ← Connect image + Debezium + ClickHouse sink (copy of poc-debezium's)
├── sql/source.sql               ← orders, signal, heartbeat tables
├── sql/meta.sql                 ← metadata DB tables (backups, jobs)
├── clickhouse/ddl.sql           ← ClickHouse tables
├── connectors/*.json            ← orders-source.json, orders-sink.json
├── scripts/count-check.sh       ← Postgres vs ClickHouse row counts
├── k8s/                         ← kind.yaml, connect.yaml, schema-registry.yaml, pdb, jobs
├── monitoring/                  ← prometheus.yml, alerts.yml
├── go.mod                       ← module github.com/debaraj/connect-reliability-lab
├── cmd/
│   ├── writer/      ← steady stream of inserts/updates (load generator)
│   ├── backup/      ← Phase 10: saves Connect offsets + schemas into the metadata DB
│   ├── metaapi/     ← Phase 14: tiny API with /ready, retries, skip-a-cycle poller
│   ├── guardian/    ← Phase 15: watchdog + Prometheus metrics, needs no DB
│   ├── extractor/   ← Phase 17: fake extractor (files → Kafka), runs as a Job
│   └── jobs/        ← Phase 16: Temporal worker + starter
└── internal/        ← shared helpers (connect REST client, config)
```

---

# Part I — Foundations

## Phase 0 — Environment & tooling

**Goal:** every tool installed, Docker sized, empty repo committed.
**Time:** 1 hour.

### 0.1 Tools

| Tool | Why | Install |
|---|---|---|
| Docker Desktop | runs everything | already installed — give it **≥ 12 GB RAM, ≥ 6 CPUs** (Settings → Resources) |
| kind | Kubernetes inside Docker (Phase 11+) | `brew install kind` (or `go install sigs.k8s.io/kind@v0.30.0`) |
| kubectl | talk to Kubernetes | already installed (v1.36) |
| jq | read JSON from REST APIs | already installed |
| psql | talk to Postgres | already installed (v15 client is fine) |
| Go 1.27 | our tools | already installed |
| Temporal CLI | Phase 16 — talk to the Temporal server (the server itself runs in Docker) | already installed |

ClickHouse and Kafka CLIs run **inside their containers** — no install needed.

### 0.2 kubectl context safety ⚠

You have real GKE contexts (`development` and `production`) in `~/.kube/config`. **Before any
`kubectl` command in this course, check the context:**

```bash
kubectl config current-context      # must print kind-lab during this course
```

Habit: always pass `--context kind-lab` in scripts and the Makefile. Never run course
commands against a GKE context.

### 0.3 Repo

```bash
cd ~/Desktop/devs/huddle/connect-reliability-lab
git init && git add -A && git commit -m "m0: course scaffold"
go mod init github.com/debaraj/connect-reliability-lab
```

**Checkpoint**
- [x] Docker shows ≥ 12 GB memory
- [x] `kind version` works
- [x] You know why every course `kubectl` command uses `--context kind-lab`

---

## Phase 1 — The big picture

**Goal:** explain the whole Connect Service and its weak spots, then carry one row through it **by hand** in Docker.
**Time:** 3 hours. **Dir:** `p01-by-hand/` (a throwaway warm-up; the real lab starts in Phase 2)

### 1.1 The pieces and their jobs

| Piece | One-line job | Runs where today |
|---|---|---|
| Source Postgres | the customer's DB; writes every change to its WAL | customer side |
| Debezium | reads the WAL, turns changes into Kafka messages | inside Connect, on the VM |
| Kafka | stores messages in order, 3 copies, for days | 5 brokers, 3 zones |
| Schema Registry | stores each table's shape once, gives it an id | on the VM |
| Kafka Connect | runs Debezium + sink, saves their bookmarks in Kafka | on the VM |
| ClickHouse sink | reads Kafka, writes batches into ClickHouse | inside Connect, on the VM |
| ClickHouse | the analytics DB the customer queries | managed |
| Go API + pollers | creates connectors, watches health, runs backfills | GKE |
| Metadata DB | the API's memory: syncs, jobs, alerts | Cloud SQL (HA) |
| Extractor | runs Airbyte connectors for Ads/GCS → Kafka | on the VM |

### 1.2 Three words you'll use all course

- **Single point of failure (SPOF)** — one part that, if it breaks, stops a big piece of the
  system. Example: the one VM running Connect.
- **RTO (recovery time)** — how long until it works again. Example: "Connect back in ~30 s".
- **RPO (data loss window)** — how much data you can lose. Our target is **0**: we may be
  *late*, never *missing* rows.

**Downtime ≠ data loss.** If Connect is down 10 minutes, rows are delayed, not lost — Postgres
holds them in the WAL. Data is lost only if something *deletes* it before it's copied (WAL
removed, Kafka retention expires, a bookmark jumps forward).

### 1.3 At-least-once + "newest wins" = effectively exactly once

Each step may send a message **twice** after a crash (it did the work but didn't save the
bookmark yet). That's fine because ClickHouse's `ReplacingMergeTree(__ts_ms)` keeps one row
per key — the newest. Two copies of "order 42 = paid" collapse into one.

**Build (paper):** draw the diagram from memory. Mark every SPOF.

### 1.4 Build — be the pipeline yourself ✅ verified

Before letting Debezium and the sink do the work, **you** do their jobs by hand. Afterwards
you'll know exactly what each component automates.

`p01-by-hand/docker-compose.yml`:

```yaml
name: p01
services:
  source-db:
    image: postgres:17-alpine
    environment: { POSTGRES_USER: app, POSTGRES_PASSWORD: app, POSTGRES_DB: shop }
    command: ["postgres", "-c", "wal_level=logical"]
  kafka:
    image: apache/kafka:4.1.1        # no settings = one broker on localhost:9092 inside the container
  clickhouse:
    image: clickhouse/clickhouse-server:26.8
    environment: { CLICKHOUSE_USER: lab, CLICKHOUSE_PASSWORD: lab, CLICKHOUSE_DB: shop, CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1" }
```

```bash
cd p01-by-hand && docker compose up -d
p()  { docker compose exec -T source-db psql -U app -d shop -tAc "$1"; }
k()  { docker compose exec -T kafka /opt/kafka/bin/$1 "${@:2}"; }
ch() { docker compose exec -T clickhouse clickhouse-client -u lab --password lab -d shop -q "$1"; }
```

**Step 1 — Postgres writes the change to its WAL; you read it through a slot (Debezium's job):**

```bash
p "CREATE TABLE orders (id bigint PRIMARY KEY, status text)"
p "SELECT pg_create_logical_replication_slot('by_hand','test_decoding')"   # its own command — see note
p "INSERT INTO orders VALUES (42,'new')"
p "UPDATE orders SET status='paid' WHERE id=42"
p "SELECT data FROM pg_logical_slot_get_changes('by_hand', NULL, NULL)"
```

Result on this Mac:

```
table public.orders: INSERT: id[bigint]:42 status[text]:'new'
table public.orders: UPDATE: id[bigint]:42 status[text]:'paid'
```

Note: a slot can't be created in a transaction that already wrote something, so don't put
it in the same `psql -c` as the `CREATE TABLE`.

**Step 2 — turn each change into a keyed Kafka message (also Debezium's job):**

```bash
k kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 1
printf '42|{"id":42,"status":"new","ver":1}\n42|{"id":42,"status":"paid","ver":2}\n' | \
  k kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders \
    --property parse.key=true --property key.separator='|'
k kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning \
  --property print.key=true --property print.offset=true --timeout-ms 6000
```

**Step 3 — copy Kafka into ClickHouse (the sink's job), keeping only the newest version:**

```bash
ch "CREATE TABLE orders (id Int64, status String, ver Int64) ENGINE=ReplacingMergeTree(ver) ORDER BY id"
k kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --timeout-ms 6000 \
  | docker compose exec -T clickhouse clickhouse-client -u lab --password lab -d shop -q "INSERT INTO orders FORMAT JSONEachRow"
ch "SELECT * FROM orders FINAL"      # → 42  paid  2
```

**Break it:** run step 3 again (copy everything twice). `SELECT count() FROM orders` goes up,
`… FINAL` still shows one row: **42, paid**. That's §1.3 in 3 commands.

**What you did by hand that the real system automates:**

| You did | Real system |
|---|---|
| read the slot with `pg_logical_slot_get_changes` | Debezium, continuously, via `pgoutput` |
| wrote JSON by hand with a `ver` number | Debezium + Avro + Schema Registry, `__ts_ms` as the version |
| remembered where you stopped (you didn't!) | Connect saves bookmarks in `connect-offsets` / consumer offsets |
| piped the consumer into ClickHouse once | the sink, in batches, forever, with retries |
| ran it all on one laptop | 3 brokers, 3 zones, several Connect workers |

Clean up: `docker compose down -v`.

**Checkpoint**
- [ ] You can draw the path of one UPDATE from Postgres to ClickHouse
- [ ] You can say why "Connect down for 10 minutes" is downtime but not data loss
- [ ] You can name 2 ways data *could* actually be lost
- [ ] You can explain why duplicates are safe in our design
- [ ] You carried one row from Postgres to ClickHouse by hand and can say which component replaces each step

---

## Phase 2 — Postgres WAL, slots and publications

**Goal:** see the WAL, create a slot by hand, and watch WAL pile up when nobody reads it.
**Time:** 3 hours. **Dir:** `docker-compose.yml`, `sql/`

### 2.1 WAL and LSN

- **WAL (write-ahead log)** — Postgres writes every change to this log *before* the table.
  It's how Postgres survives crashes, and how CDC reads changes.
- **LSN** — a position in the WAL, like a page number: `0/19D3698`. Bigger = later.
- **`wal_level=logical`** — makes the WAL detailed enough to rebuild row changes. Required for
  CDC. Cloud SQL: flag `cloudsql.logical_decoding=on` (our prod instance has it on).

### 2.2 Replication slot

A **slot** is a bookmark that Postgres keeps *for a reader*. Postgres will not delete WAL
past the slot's `confirmed_flush_lsn`.

- Good: Debezium can stop for an hour and lose nothing.
- Danger: if the reader never comes back, WAL grows **until the disk is full** and the
  customer's DB stops. This is why we alert on slot lag.

### 2.3 Publication and pgoutput

- **Publication** — the list of tables a slot should emit. `CREATE PUBLICATION orders_pub FOR TABLE orders;`
- **pgoutput** — the built-in plugin that turns WAL into row changes (`plugin.name: pgoutput`).
- **REPLICA IDENTITY** — what the WAL records for UPDATE/DELETE "before" values. Default =
  primary key only. A table without a primary key can't send UPDATE/DELETE properly.

🏭 **In our code:** `cdc-etl-connect/internal/source/postgres/validator.go` builds the Debezium
config (`slot.name`, `publication.name`, `publication.autocreate.mode`). Our DB Monitoring
module (`internal/pgmonitor`) alerts on slot lag.

### 2.4 Build — source Postgres

Start `docker-compose.yml` with just the source DB:

```yaml
name: lab
services:
  source-db:
    image: postgres:17-alpine
    hostname: source-db
    ports: ["5433:5432"]
    environment: { POSTGRES_USER: app, POSTGRES_PASSWORD: app, POSTGRES_DB: shop }
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"]
```

`sql/source.sql`:

```sql
CREATE TABLE orders (
  id bigserial PRIMARY KEY,
  customer text NOT NULL,
  amount numeric(10,2) NOT NULL,
  status text NOT NULL DEFAULT 'new',
  updated_at timestamp NOT NULL DEFAULT now()
);
CREATE TABLE debezium_signal (id varchar(64) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048));
CREATE TABLE debezium_heartbeat (id int PRIMARY KEY, ts timestamptz NOT NULL);
INSERT INTO debezium_heartbeat VALUES (1, now());
INSERT INTO orders (customer, amount) SELECT 'cust-'||g, g*1.5 FROM generate_series(1,1000) g;
```

Load it: `docker compose exec -T source-db psql -U app -d shop < sql/source.sql`

### 2.5 Build — a slot by hand

```sql
SELECT pg_create_logical_replication_slot('hand_slot', 'test_decoding');
UPDATE orders SET status='paid' WHERE id=42;
SELECT * FROM pg_logical_slot_peek_changes('hand_slot', NULL, NULL);   -- look, don't consume
SELECT slot_name, active, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots;
```

(`test_decoding` prints readable text; Debezium uses `pgoutput`, which is binary.)

### 2.6 Break it — WAL that never goes away

1. Run 200k updates: `UPDATE orders SET amount = amount + 1;` a few times.
2. Check `lag` again — it grows, because nobody reads `hand_slot`.
3. `SELECT pg_drop_replication_slot('hand_slot');` — lag disappears.

**Second scenario:** a busy shared Postgres where *our* tables are quiet. The slot doesn't
move (no changes for us), but WAL from *other* tables piles up behind it. Fix = **heartbeat**
(Phase 7).

**Checkpoint**
- [ ] You can explain what a slot protects and what it endangers
- [ ] You know why a table needs a primary key for CDC
- [ ] You can read slot lag with one SQL query
- [ ] You can explain why a quiet table on a busy DB still grows WAL

---

## Phase 3 — Kafka basics

**Goal:** produce and consume messages; understand topics, partitions, offsets, keys,
consumer groups, retention.
**Time:** 3 hours.

### 3.1 Concepts

- **Topic** — a named, append-only log. Example: `lab.public.orders`.
- **Partition** — a topic is split into N logs so work can be spread. Order is guaranteed
  **within** a partition only.
- **Offset** — a message's position in a partition (0, 1, 2…). Consumers remember "I'm at 327".
- **Key** — decides the partition (`hash(key) % partitions`). Same key → same partition →
  in order. We key by primary key, so all changes for `id=42` stay in order.
- **Consumer group** — consumers sharing one name split the partitions between them. Each
  partition goes to one member. The ClickHouse sink's group is `connect-<connector name>`.
- **Retention** — how long Kafka keeps messages (e.g. 7 days), even after they're read.
  This is our safety window if the sink is broken.

🏭 **In our code:** topics are capped at 24 partitions; retention is set per topic in days
(`internal/kafkametrics/client.go`, broker default read in `defaults.go`).

### 3.2 Build — one broker first

Add a single `kafka-1` from §4.4 (you'll grow it to 3 in Phase 4). Then:

```bash
# helper: run Kafka CLI tools inside the broker container
k() { docker compose exec -T kafka-1 /opt/kafka/bin/$1 "${@:2}"; }

k kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic demo --partitions 3 --replication-factor 1
printf 'a:1\nb:2\na:3\n' | k kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic demo \
   --property parse.key=true --property key.separator=:
k kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic demo --from-beginning \
   --property print.key=true --property print.partition=true --timeout-ms 5000
```

Notice both `a` messages land in the same partition.

### 3.3 Break it — consumer groups

Run two consumers with `--group g1` in two terminals; produce 20 keyed messages. Each
consumer gets some partitions. Kill one — the other takes over its partitions.

**Checkpoint**
- [ ] Why does keying by primary key keep updates for one row in order?
- [ ] If a topic has 3 partitions, what's the most consumers in one group that do useful work?
- [ ] Why does retention matter when the sink is down for 2 days?

---

## Phase 4 — Kafka durability: 3 brokers, 3 zones

**Goal:** a 3-broker cluster where losing one broker loses nothing; know exactly what
happens when you lose two.
**Time:** 4 hours. This is the foundation of "zero data loss".

### 4.1 Concepts

- **Replication factor (RF) = 3** — every partition is stored on 3 brokers.
- **Leader / followers** — one copy takes writes (leader); the others copy it.
- **ISR (in-sync replicas)** — copies that are fully caught up.
- **`min.insync.replicas=2`** — a write with `acks=all` succeeds only if ≥ 2 copies have it.
- **`acks=all`** — the producer waits until all in-sync copies have it. With min ISR 2, a
  "success" means at least 2 brokers have the message. Connect uses `CONNECT_PRODUCER_ACKS: all`.
- **`unclean.leader.election.enable=false`** — never make an out-of-date copy the leader
  (that would silently drop messages).
- **Rack awareness (`broker.rack`)** — tell Kafka each broker's zone, so the 3 copies land in
  3 different zones.
- **KRaft** — Kafka's built-in voting system for cluster metadata (no ZooKeeper). Needs a
  **majority** of controllers alive: 2 of 3.
- **Bootstrap servers** — the addresses a client uses *only to first find the cluster*. If you
  list one and it's down, the client can't start.

🏭 **In our code:** production has 5 brokers on 3 nodes in zones a/b/c with RF 3, min ISR 2,
unclean election off. Connect's bootstrap in `poc-debezium/.env` is still **one address** —
red mark 2 on the plan page.

### 4.2 The rule of thumb

| Brokers alive (of 3) | `acks=all` writes (min ISR 2) | Data lost? |
|---|---|---|
| 3 | ✅ work | no |
| 2 | ✅ work | no |
| 1 | ❌ refused (`NOT_ENOUGH_REPLICAS`) | no — refused, not lost |

Refusing a write is **safe**: the producer retries, and Postgres keeps the change in the WAL.

### 4.3 Generate a cluster id

```bash
docker run --rm apache/kafka:4.1.1 /opt/kafka/bin/kafka-storage.sh random-uuid
```

### 4.4 Build — 3 brokers ✅ verified

```yaml
x-kafka: &kafka
  image: apache/kafka:4.1.1
  restart: unless-stopped
x-kafka-env: &kafka-env
  CLUSTER_ID: "<paste the uuid>"
  KAFKA_PROCESS_ROLES: broker,controller        # combined mode: fine for a laptop, not for prod
  KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
  KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
  KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3
  KAFKA_MIN_INSYNC_REPLICAS: 2
  KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
  KAFKA_LOG_RETENTION_HOURS: 168                # 7 days
  KAFKA_HEAP_OPTS: "-Xms512m -Xmx512m"

services:
  kafka-1:
    <<: *kafka
    hostname: kafka-1
    ports: ["19092:19092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_BROKER_RACK: zone-a
      KAFKA_LISTENERS: INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:19092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:9092,EXTERNAL://localhost:19092
  # kafka-2: same, NODE_ID 2, RACK zone-b, port 29092
  # kafka-3: same, NODE_ID 3, RACK zone-c, port 39092
```

Two listeners: `INTERNAL` (`kafka-1:9092`) for containers, `EXTERNAL` (`localhost:19092`) for
Go tools on your Mac. Bootstrap for Mac tools: `localhost:19092,localhost:29092,localhost:39092`.

Check the spread:

```bash
k kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic orders-test --partitions 6
k kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders-test
k kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
```

Every partition shows `Replicas: x,y,z` on 3 brokers and a different leader.

### 4.5 Break it ✅ verified

1. **One broker down** — `docker compose stop kafka-2`. Produce with `acks=all` → works.
2. **Too few copies** — create a topic with only 2 copies:
   `--create --topic rf2 --replica-assignment 1:3`, stop `kafka-3`, then produce with
   `--producer-property acks=all` → error (`NOT_ENOUGH_REPLICAS`). Same with `acks=1` → works.
   *Lesson:* `acks=all` + min ISR 2 refuses writes instead of risking loss.
3. **Two brokers down** — stop `kafka-2` and `kafka-3`. Even `kafka-topics.sh --list` times
   out: the KRaft quorum (2 of 3 voters) is gone, so the cluster can't do anything.
4. **One bootstrap address** — stop `kafka-2`, then run
   `kafka-topics.sh --bootstrap-server kafka-2:9092 --list` → hangs/fails. With
   `--bootstrap-server kafka-2:9092,kafka-1:9092,kafka-3:9092` → works.

Restart everything (`docker compose start`) and check ISR returns to 3.

**Second scenario:** a zone outage takes out one node that hosts 2 of your 5 production
brokers. Rack awareness means each partition still has ≥ 2 copies in the other zones.

**Checkpoint**
- [ ] Why is "write refused" safe but "unclean leader election" not?
- [ ] With RF 3 and min ISR 2, how many brokers can die before writes stop?
- [ ] Why did 2 dead brokers stop even *reads of metadata* in our lab?
- [ ] Why must Connect be given 3 bootstrap addresses, not 1?

---

## Phase 5 — Schema Registry and Avro

**Goal:** register schemas, see ids and versions, run 2 registries and survive losing one.
**Time:** 3 hours.

### 5.1 Concepts

- **Avro** — a compact binary format. The message doesn't carry column names, only values.
- **Schema** — the shape of a row: field names and types.
- **Subject** — a named list of schema versions, e.g. `lab.public.orders-value` (and `-key`).
- **Schema id** — a global number for one exact schema. Each message starts with 1 magic
  byte + the 4-byte id, then the data. Readers look up the id to decode.
- **Compatibility** — rules for changing a schema (e.g. `BACKWARD`: new readers can read old
  data). Adding a nullable column with a default is safe; renaming is not.
- **`_schemas` topic** — where the registry stores everything. The registry itself keeps no
  disk state; it rebuilds from this topic on start.
- **Leader** — with several registries, exactly one is the leader (chosen through Kafka). Any
  instance can **read**; **writes are forwarded** to the leader.

🏭 **In our code:** `poc-debezium/docker-compose.yml` runs `cp-schema-registry:8.3.0` on the
VM. `cdc-etl-connect/internal/schemaregistry` talks to it.

### 5.2 Build — two registries ✅ verified

```yaml
  schema-registry-1: &sr
    image: confluentinc/cp-schema-registry:8.3.0
    hostname: schema-registry-1
    ports: ["8081:8081"]
    depends_on: [kafka-1, kafka-2, kafka-3]
    environment: &sr-env
      SCHEMA_REGISTRY_HOST_NAME: schema-registry-1        # must be reachable by the other instance!
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 3
      SCHEMA_REGISTRY_LEADER_ELIGIBILITY: "true"
      SCHEMA_REGISTRY_HEAP_OPTS: "-Xms256m -Xmx256m"
  schema-registry-2:
    <<: *sr
    hostname: schema-registry-2
    ports: ["8082:8081"]
    environment:
      <<: *sr-env
      SCHEMA_REGISTRY_HOST_NAME: schema-registry-2
```

Register and read:

```bash
S='{"schema":"{\"type\":\"record\",\"name\":\"T\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}"}'
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data "$S" \
  localhost:8082/subjects/test-value/versions          # written via instance 2
curl -s localhost:8081/subjects/test-value/versions    # read via instance 1
docker compose logs schema-registry-1 | grep -i "leader"   # who is the leader?
```

### 5.3 Break it ✅ verified

Stop the leader (`docker compose stop schema-registry-1`), then register version 2 (add
`{"name":"b","type":["null","string"],"default":null}`) through `:8082`. It succeeds — the
other instance became leader.

**Why `HOST_NAME` matters:** the follower forwards writes to the leader's `HOST_NAME`. If that
name can't be reached from the follower, writes through the follower fail. Same lesson as
Connect's advertised host name in Phase 13.

Clients list both: `schema.registry.url: http://schema-registry-1:8081,http://schema-registry-2:8081`.

**Checkpoint**
- [ ] Why doesn't a schema registry need its own disk?
- [ ] If the leader dies, what happens to reads? to writes?
- [ ] Why would a message become unreadable if `_schemas` were lost?

---

## Phase 6 — Kafka Connect

**Goal:** run Connect in distributed mode, use its REST API, and watch work move between
workers.
**Time:** 4 hours.

### 6.1 Concepts

- **Worker** — one running Connect process.
- **Connector** — a configured plugin instance (Debezium for `orders`, sink for `orders`).
- **Task** — the thread that does the actual copying. A connector has 1..`tasks.max` tasks.
  Debezium Postgres always uses 1 task (one slot = one reader).
- **Converter** — turns Kafka bytes ↔ rows (we use `AvroConverter` + Schema Registry).
- **SMT (single message transform)** — small change on the way through, e.g. `unwrap`.
- **Internal topics** — Connect's memory, all in Kafka:
  - `connect-configs` — connector configs (+ pause/stop state)
  - `connect-offsets` — **source** bookmarks (e.g. Debezium's LSN)
  - `connect-status` — RUNNING / FAILED / PAUSED
  - **Sink** bookmarks are normal consumer-group offsets (group `connect-<name>`).
- **Distributed mode** — workers with the same `group.id` form one cluster and share
  connectors. One worker is the **leader** and decides who runs what.
- **Rebalance** — redistributing work when workers join or leave. Since Kafka 2.3 it is
  **incremental cooperative**: only the work that must move, moves.
- **`scheduled.rebalance.max.delay.ms`** — when a worker *disappears* (crash), wait this long
  for it to come back before giving its work away. Default 5 min; we use 30 s.
- **Advertised host name** — the address a worker tells others to use for its REST API. The
  others **forward** requests to the leader using it, so it must be reachable from every
  worker.

### 6.2 REST API (port 8083) — the ones we use

| Call | What |
|---|---|
| `GET /connectors?expand=status` | all connectors + task states + which worker |
| `PUT /connectors/{n}/config` | create or update |
| `PUT /connectors/{n}/pause` · `/resume` · `/stop` | pause keeps tasks assigned; **stop** shuts tasks down (needed to edit offsets) |
| `POST /connectors/{n}/restart?includeTasks=true&onlyFailed=true` | restart only failed parts (our watchdog uses this) |
| `GET /connectors/{n}/offsets` | read bookmarks |
| `PATCH` / `DELETE /connectors/{n}/offsets` | change / reset bookmarks — **connector must be STOPPED** |

🏭 **In our code:** `cdc-etl-connect/internal/connect` is the REST client. Worker settings live
in `poc-debezium/docker-compose.yml` (`CONNECT_GROUP_ID`, the 3 internal topics with RF 3,
`CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS: 30000`, `CONNECT_PRODUCER_ACKS: all`,
`CONNECT_REST_ADVERTISED_HOST_NAME: connect`).

### 6.3 Build — the Connect image

Copy the real `poc-debezium/Dockerfile` to `connect/Dockerfile`. It downloads Debezium
3.6.0.Final and the ClickHouse sink 1.4.0 with SHA-256 checks into
`cp-kafka-connect-base:8.3.0`. Read every line and explain it in `NOTES.md`.

### 6.4 Build — one worker ("the VM") ✅ verified

```yaml
  connect-vm:
    build: ./connect
    hostname: connect-vm
    ports: ["8083:8083"]
    depends_on: [schema-registry-1]
    environment: &connect-env
      CONNECT_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
      CONNECT_GROUP_ID: lab-connect
      CONNECT_CONFIG_STORAGE_TOPIC: connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: connect-status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_KEY_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry-1:8081,http://schema-registry-2:8081
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry-1:8081,http://schema-registry-2:8081
      CONNECT_REST_ADVERTISED_HOST_NAME: connect-vm
      CONNECT_REST_PORT: 8083
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components
      CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS: 30000
      CONNECT_PRODUCER_ACKS: all
      CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
      KAFKA_HEAP_OPTS: "-Xms768m -Xmx768m"
```

```bash
curl -s localhost:8083/connector-plugins | jq -r '.[].class'
# expect PostgresConnector and ClickHouseSinkConnector in the list
k kafka-topics.sh --bootstrap-server kafka-1:9092 --list   # connect-configs/offsets/status appeared
```

Note: the Connect image has **no `curl` inside**. Call its REST API from your Mac
(`localhost:8083`) or from a throwaway container: `docker run --rm --network lab_default alpine wget -qO- http://connect-vm:8083/`.

Work-moving experiments come in Phase 9 once connectors exist.

**Checkpoint**
- [ ] Where does Connect keep a source bookmark? a sink bookmark?
- [ ] What's the difference between pause and stop?
- [ ] Why must the advertised host name be reachable by *other* workers?
- [ ] After a crash, why does a task wait up to 30 s before moving?

---

## Phase 7 — Debezium Postgres source

**Goal:** stream `orders` changes into Kafka as Avro, with heartbeats and a signal table.
**Time:** 4 hours.

### 7.1 Concepts

- **Snapshot** — on first start (`snapshot.mode: initial`), Debezium copies existing rows,
  then switches to streaming live changes.
- **Streaming** — reads the slot via `pgoutput`, one event per row change.
- **Event shape** — Debezium's raw event has `before`, `after`, `op`, `source`… The **`unwrap`**
  SMT (`ExtractNewRecordState`) keeps just the new row. `add.fields: op,ts_ms` adds `__op`
  (`c`/`u`/`d`/`r`=snapshot read) and `__ts_ms` (change time). Deletes with
  `delete.tombstone.handling.mode: rewrite` become a row with `__deleted = "true"`.
- **Offset** — Debezium stores the last LSN in `connect-offsets` and tells Postgres "free WAL
  up to here" (`lsn.flush.mode: connector`, the default).
- **Heartbeat** — `heartbeat.interval.ms` (default 0 = off) makes Debezium send its position
  even when our tables are quiet; `heartbeat.action.query` writes a tiny row so the slot has
  something to move past. Fixes the "quiet table on busy DB" problem from §2.6.
- **Signal table** — `signal.data.collection`: insert a row there to tell Debezium "re-copy
  table X" (incremental snapshot) without a restart. This is how backfills work.
- **`errors.max.retries`** — for *retriable* errors (e.g. DB connection dropped):
  `-1` (default) = keep restarting, `0` = fail at once, `N` = N restarts then FAILED.

🏭 **In our code:** `cdc-etl-connect/internal/source/postgres/validator.go` (~line 400) sets
`heartbeat.interval.ms: 10000`, `transforms.unwrap.add.fields: op,ts_ms`, the signal table and
heartbeat query. Template: `poc-debezium/connectors/postgres-source.json.template`.

### 7.2 Build — `connectors/orders-source.json` ✅ verified

```json
{"name":"orders-source","config":{
 "connector.class":"io.debezium.connector.postgresql.PostgresConnector",
 "database.hostname":"source-db","database.port":"5432","database.user":"app","database.password":"app","database.dbname":"shop",
 "plugin.name":"pgoutput","slot.name":"orders_slot","publication.name":"orders_pub","publication.autocreate.mode":"filtered",
 "table.include.list":"public.orders,public.debezium_signal","topic.prefix":"lab","snapshot.mode":"initial",
 "decimal.handling.mode":"double","time.precision.mode":"connect",
 "signal.data.collection":"public.debezium_signal",
 "heartbeat.interval.ms":"10000","heartbeat.action.query":"UPDATE debezium_heartbeat SET ts = now() WHERE id = 1",
 "topic.creation.default.replication.factor":"3","topic.creation.default.partitions":"3",
 "transforms":"unwrap","transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState",
 "transforms.unwrap.delete.tombstone.handling.mode":"rewrite","transforms.unwrap.add.fields":"op,ts_ms"}}
```

`topic.creation.default.*` is needed because the brokers have auto-create **off** — Connect
creates the topics itself with RF 3.

```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connectors/orders-source.json localhost:8083/connectors
curl -s localhost:8083/connectors/orders-source/status | jq
curl -s localhost:8081/subjects        # lab.public.orders-key / -value appeared
```

Read the topic as Avro:

```bash
docker compose exec schema-registry-1 kafka-avro-console-consumer \
  --bootstrap-server kafka-1:9092 --topic lab.public.orders --from-beginning --max-messages 3 \
  --property schema.registry.url=http://schema-registry-1:8081
```

### 7.3 Break it

1. Update and delete a row; read the topic — see `__op: u` and `__deleted: "true"`.
2. `PUT /connectors/orders-source/stop`, run 1000 updates, check slot lag grows (§2.5).
   `resume` — lag drains. Nothing lost.
3. Trigger a re-copy: `INSERT INTO debezium_signal VALUES ('s1','execute-snapshot','{"data-collections":["public.orders"],"type":"incremental"}');`
   Watch `__op: r` events appear while live changes keep flowing.

**Checkpoint**
- [ ] When does Debezium tell Postgres it may free WAL?
- [ ] Why do we need both `heartbeat.interval.ms` and `heartbeat.action.query`?
- [ ] What does the signal table let you do without a restart?
- [ ] What does `errors.max.retries: -1` do when the source DB restarts?

---

## Phase 8 — ClickHouse and the sink

**Goal:** finish the "today" pipeline end to end and prove duplicates are harmless.
**Time:** 4 hours.

### 8.1 Concepts

- **MergeTree** — ClickHouse's main table engine. Data is written in *parts* and merged in
  the background.
- **ReplacingMergeTree(ver)** — during merges, keeps **one row per ORDER BY key**, the one
  with the biggest `ver`. We use `ReplacingMergeTree(__ts_ms) ORDER BY id`.
- **`FINAL`** — `SELECT … FROM t FINAL` merges at query time, so you see one row per key even
  before background merges run. Without `FINAL` you may see duplicates.
- **ClickHouse sink connector** — reads topics (`topics`), maps topic → table
  (`topic2TableMap`), inserts in big batches, then commits its consumer offsets.
- **At-least-once** — with `exactlyOnce: false` a crash can re-insert the last batch. Safe,
  thanks to ReplacingMergeTree.
- **`errors.tolerance`** — `none` (our default): one bad message stops the task (FAILED). `all`:
  skip it (optionally to a **dead letter queue** topic). We'll revisit in Phase 18.

🏭 **In our code:** default engine `ReplacingMergeTree(__ts_ms)` in
`cdc-etl-connect/internal/sink/clickhouse/table_layout.go`; required sink settings
(`errors.tolerance: none`, `reportInsertedOffsets: true`) in `trim_delivery.go`; sink tasks = 1
today.

### 8.2 Build ✅ verified

```yaml
  clickhouse:
    image: clickhouse/clickhouse-server:26.8
    hostname: clickhouse
    ports: ["8123:8123", "9000:9000"]
    environment: { CLICKHOUSE_USER: lab, CLICKHOUSE_PASSWORD: lab, CLICKHOUSE_DB: shop, CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1" }
    ulimits: { nofile: { soft: 262144, hard: 262144 } }
```

`clickhouse/ddl.sql`:

```sql
CREATE TABLE orders (
  id Int64, customer String, amount Float64, status String, updated_at DateTime64(3),
  __op String, __ts_ms Int64, __deleted String
) ENGINE = ReplacingMergeTree(__ts_ms) ORDER BY id;
```

`connectors/orders-sink.json`:

```json
{"name":"orders-sink","config":{
 "connector.class":"com.clickhouse.kafka.connect.ClickHouseSinkConnector","tasks.max":"1",
 "topics":"lab.public.orders","topic2TableMap":"lab.public.orders=orders",
 "hostname":"clickhouse","port":"8123","database":"shop","username":"lab","password":"lab","ssl":"false",
 "exactlyOnce":"false","consumer.override.auto.offset.reset":"earliest","errors.tolerance":"none"}}
```

```bash
ch() { docker compose exec -T clickhouse clickhouse-client -u lab --password lab -d shop -q "$1"; }
ch "SELECT count() FROM orders FINAL WHERE __deleted='false'"     # = Postgres count
```

Write `scripts/count-check.sh` that prints Postgres count, ClickHouse `FINAL` live count, and
`OK`/`MISMATCH`. You'll run it after every drill.

### 8.3 Break it — replay everything ✅ verified

```bash
curl -X PUT localhost:8083/connectors/orders-sink/stop
curl -s localhost:8083/connectors/orders-sink/offsets | jq        # see kafka_offset per partition
curl -X PATCH -H 'Content-Type: application/json' localhost:8083/connectors/orders-sink/offsets --data \
 '{"offsets":[{"partition":{"kafka_topic":"lab.public.orders","kafka_partition":0},"offset":{"kafka_offset":0}},
              {"partition":{"kafka_topic":"lab.public.orders","kafka_partition":1},"offset":{"kafka_offset":0}},
              {"partition":{"kafka_topic":"lab.public.orders","kafka_partition":2},"offset":{"kafka_offset":0}}]}'
curl -X PUT localhost:8083/connectors/orders-sink/resume
```

Result on this Mac: raw `count()` went to **2001**, `count() FINAL` stayed **999**. Every row
was inserted twice; ReplacingMergeTree hid it.

**Second scenario:** a GCS file is re-read after a crash. Same trick — the row key + a
version column make the re-read harmless (our GCS path uses file position as the key).

**Checkpoint**
- [ ] Why can the sink insert a batch twice?
- [ ] Why does `FINAL` give the right count when raw `count()` doesn't?
- [ ] What happens to *all* tables in a sink if one message is bad and `errors.tolerance=none`?
- [ ] 🎉 You can draw today's system and you've built it locally.

---

# Part II — Reliability upgrades

## Phase 9 — Failure thinking and baseline drills

**Goal:** measure how today's setup fails, *before* fixing anything. Learn to never trust a
status field alone.
**Time:** 3 hours.

### 9.1 Concepts

- **Failure table** — for each failure: what breaks, how long, any data loss. The plan page's
  "What if it breaks?" table is one.
- **Drill** — breaking something on purpose in a safe place and measuring the result.
- **Evidence** — row counts (`count-check.sh`), slot lag, sink lag, worker logs. Not status.
- **Graceful stop vs crash** — a graceful stop (SIGTERM) lets a worker commit offsets and
  leave the group at once. A crash (SIGKILL) leaves the group only after a timeout.

### 9.2 Build — a load generator

`cmd/writer/main.go`: connect to `localhost:5433` with `pgx`, every 200 ms insert a row and
update a random one. Flags: `-rate`, `-duration`. You'll run it during every drill so failures
happen *under load*.

### 9.3 Build — a second worker (stage 2 preview) ✅ verified

Add `connect-2` (same env as `connect-vm`, `CONNECT_REST_ADVERTISED_HOST_NAME: connect-2`, port
`8084:8083`). Start it and run:

```bash
curl -s 'localhost:8083/connectors?expand=status' | jq -c 'to_entries[]|{n:.key,t:[.value.status.tasks[].worker_id]}'
```

Result on this Mac: when `connect-2` joined, **only the source moved** to it; the sink stayed
on `connect-vm`. That's incremental cooperative rebalancing.

### 9.4 Break it — crash vs graceful ✅ verified

With the writer running:

| Drill | Command | Result on this Mac |
|---|---|---|
| Crash the worker running the source | `docker kill lab-connect-2-1` | status kept saying **RUNNING on connect-2 for ~40 s**, then the task moved to `connect-vm` (~45–50 s total). Counts matched: 1059 = 1059 |
| Gracefully stop a worker | `docker stop -t 60 lab-connect-vm-1` | tasks moved in **~3 s** |

Two lessons:

1. **After a crash, status lies for a while.** The dead worker's tasks still show RUNNING
   until the group notices (session timeout) and the 30 s delay passes. A watchdog that only
   reads status can't see this — you also need **lag** (Phase 15).
2. **Always stop gracefully** (`stop_grace_period: 60s` in compose,
   `terminationGracePeriodSeconds: 60` in Kubernetes). It's 15× faster and avoids re-sending
   the last batch.

### 9.5 Break it — a FAILED task ✅ verified

```bash
ch "RENAME TABLE orders TO orders_hidden"
# insert a row in Postgres, wait 40 s
curl -s localhost:8083/connectors/orders-sink/status | jq -c '{c:.connector.state,t:[.tasks[].state]}'
# → {"c":"RUNNING","t":["FAILED"]}   ← connector RUNNING, task FAILED!
ch "RENAME TABLE orders_hidden TO orders"
curl -X POST 'localhost:8083/connectors/orders-sink/restart?includeTasks=true&onlyFailed=true'   # 202
```

The task came back and the missing row arrived. **Check tasks, not just the connector.** A
FAILED task never fixes itself — someone (today: our API's auto-restart, which needs the
metadata DB) must restart it.

**Checkpoint**
- [ ] Why can status say RUNNING for a worker that's dead?
- [ ] Why is a graceful stop much faster than a crash?
- [ ] Connector RUNNING + task FAILED — is data flowing?
- [ ] Write your own "today" failure table in `NOTES.md` with the numbers you measured

---

## Phase 10 — Kafka safety net: addresses, retention, backups

**Goal:** fix red mark 2 and build the backup job for Connect bookmarks and schemas.
**Time:** 6 hours. Plan page: **Upgrade 2**.

### 10.1 Bootstrap list

Change every client to all brokers: Connect, Schema Registry, our Go tools.
In production: `KAFKA_BOOTSTRAP_SERVERS=<b1>:9092,<b2>:9092,<b3>:9092` — pick brokers in
**different zones**. Re-run §4.5 drill 4 to prove it.

### 10.2 Retention as a safety window

If the sink is broken for 3 days and retention is 2 days, day-1 messages are **deleted
before ClickHouse got them** — real data loss. Rule: retention ≥ 7 days, and during
migrations pause any topic trimming.

🏭 **In our code:** retention is per topic (`internal/kafkametrics/client.go`); the platform
also trims topics (`topic_trim` poller) — pause it during the move.

### 10.3 Why back up `connect-offsets` and `_schemas`?

Kafka keeps 3 copies, so a broker dying is fine. Backups protect against **losing the whole
cluster** or **a human mistake** (topic deleted, wrong offset PATCH). Without them:

- no `connect-offsets` → Debezium doesn't know its LSN → full re-snapshot of every table.
- no `_schemas` → old messages can't be decoded (the id in each message points nowhere).

### 10.4 How to back up and restore

- **Offsets:** `GET /connectors/{n}/offsets` for every connector. Restore = create the
  connector **stopped** (`"initial_state":"STOPPED"` in the create body), `PATCH` the saved
  offsets, then `resume`.
- **Schemas:** for each subject and version, save `{subject, version, id, schemaType,
  schema}`. Restore into an **empty** registry in **IMPORT mode** so ids stay the same:
  `PUT /mode {"mode":"IMPORT"}` → `POST /subjects/{s}/versions` with `id` + `version` →
  `PUT /mode {"mode":"READWRITE"}`. (Into a non-empty registry: per-subject IMPORT, which
  needs `mode.mutability=true`.)

### 10.5 Build — metadata DB + `cmd/backup`

Add a second Postgres (`meta-db`, port 5434) — this plays our Cloud SQL metadata DB.
`sql/meta.sql`:

```sql
CREATE TABLE connector_offset_backups (
  id bigserial PRIMARY KEY, connector text NOT NULL, taken_at timestamptz NOT NULL DEFAULT now(),
  config jsonb NOT NULL, offsets jsonb NOT NULL
);
CREATE TABLE schema_backups (
  id bigserial PRIMARY KEY, taken_at timestamptz NOT NULL DEFAULT now(),
  subject text NOT NULL, version int NOT NULL, schema_id int NOT NULL,
  schema_type text NOT NULL, schema text NOT NULL,
  UNIQUE (subject, version, schema_id)
);
```

`cmd/backup` (Go): every 5 minutes (flag `-every`), list connectors → save config + offsets;
list subjects → versions → insert new ones (`ON CONFLICT DO NOTHING`). Second mode
`-restore-schemas -to http://localhost:8091` replays them in IMPORT mode.

### 10.6 Break it — restore drill ✅ verified (schemas part)

Start an empty registry on a **different store topic** so it acts like a new cluster:

```bash
docker run -d --rm --name sr-restore --network lab_default -p 8091:8081 \
  -e SCHEMA_REGISTRY_HOST_NAME=sr-restore -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  -e SCHEMA_REGISTRY_KAFKASTORE_TOPIC=_schemas_restore -e SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID=sr-restore \
  -e SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR=3 confluentinc/cp-schema-registry:8.3.0
```

Run your restore, then compare: `curl localhost:8081/subjects/lab.public.orders-value/versions/latest`
vs `:8091` — **same `id`**. (Verified with a shell loop on this Mac: all 6 schemas kept their
ids. On CP 8.3 a global `IMPORT` on an empty registry needed no extra setting.)

Offsets drill: delete `orders-sink`, recreate it with `"initial_state":"STOPPED"`, PATCH the
saved offsets, resume, run `count-check.sh`.

**Checkpoint**
- [ ] What two things break if `_schemas` is lost?
- [ ] Why must the restored registry be empty for a global IMPORT?
- [ ] Why must a connector be STOPPED before PATCHing offsets?
- [ ] Why is "retention shorter than the outage" real data loss?

---

## Phase 11 — Kubernetes basics on kind

**Goal:** know enough Kubernetes to run Connect safely across 3 zones.
**Time:** 5 hours.

### 11.1 Concepts (one line each)

| Word | Meaning | Example |
|---|---|---|
| Node | one machine (here: one Docker container) | `lab-worker2` in zone-b |
| Pod | one or more containers that run together | one Connect worker |
| Deployment | keeps N identical pods running, replaces dead ones | `replicas: 3` |
| Service | a stable name for a set of pods | `schema-registry.streaming.svc` |
| Secret / ConfigMap | settings and passwords given to pods | Kafka password |
| Labels / selectors | tags used to pick pods or nodes | `app: connect`, `pool: connect` |
| Readiness probe | "send me traffic only when this check passes" | `GET /connectors` |
| Liveness probe | "restart me if this check fails" | use carefully for JVMs |
| Requests / limits | reserved CPU/memory, and the hard ceiling | `memory: 1536Mi` |
| Node pool | a group of nodes for one kind of work | dedicated pool for Connect |
| Topology spread | spread pods evenly across zones | 1 Connect per zone |
| PodDisruptionBudget (PDB) | "never voluntarily take down more than X" | `minAvailable: 2` |
| Job | run a pod to completion (with retries) | one extraction run |
| Drain | evict pods from a node before maintenance | GKE node upgrade |

**Voluntary vs involuntary:** a PDB protects against *planned* evictions (drain, upgrade). It
can't stop a node crashing — that's what 3 replicas across zones are for.

### 11.2 Build — a 3-zone cluster ✅ verified

`k8s/kind.yaml`:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: lab
nodes:
  - role: control-plane
  - role: worker
    labels: { topology.kubernetes.io/zone: zone-a, pool: connect }
  - role: worker
    labels: { topology.kubernetes.io/zone: zone-b, pool: connect }
  - role: worker
    labels: { topology.kubernetes.io/zone: zone-c, pool: connect }
```

```bash
kind create cluster --config k8s/kind.yaml
kubectl --context kind-lab get nodes -L topology.kubernetes.io/zone
```

### 11.3 Practice

Deploy `nginx` with 3 replicas + a topology spread rule + a PDB `minAvailable: 2`. Then:
`kubectl drain lab-worker --ignore-daemonsets` (allowed), and while it's drained try draining
`lab-worker2` (blocked by the PDB). `kubectl uncordon` both.

**Checkpoint**
- [ ] Deployment vs Job — when would you use each?
- [ ] What does a PDB protect against, and what can't it protect against?
- [ ] Why use topology spread instead of just `replicas: 3`?
- [ ] Why can a too-strict liveness probe make a slow-starting JVM loop forever?

---

## Phase 12 — Connect and Schema Registry on Kubernetes

**Goal:** 3 Connect workers + 2 Schema Registry pods, spread over 3 zones, safe to drain.
**Time:** 6 hours. Plan page: **Upgrade 1**.

### 12.1 The design choices (and why)

| Choice | Why |
|---|---|
| **Deployment** with 3 replicas, same `CONNECT_GROUP_ID` | workers are stateless — state is in Kafka |
| Advertised host = **pod IP** (Downward API `status.podIP`) | every worker must reach the leader's REST API |
| `topologySpreadConstraints` on zone | one zone down → 2 workers keep going |
| PDB `minAvailable: 2` | node upgrades never take 2 workers at once |
| Readiness on `GET /connectors` | only route REST calls to a worker that answers |
| **No CPU autoscaling** | adding/removing workers = rebalances = pauses; size it fixed |
| Dedicated node pool (`nodeSelector` + taint/toleration) | noisy neighbours can't starve Connect |
| `terminationGracePeriodSeconds: 60` | graceful stop: ~3 s move instead of ~45 s |
| Secrets for passwords | not in the image, not in git |
| Schema Registry: 2 pods, `HOST_NAME` = pod IP | writes are forwarded to the leader by that name |
| **Cloud NAT + static IP** (GKE only) | all pods leave GCP from one fixed IP → add it to source DB, Kafka and ClickHouse allowlists |

### 12.2 Build — `k8s/connect.yaml` ✅ verified (with the lab-only parts marked)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: connect, namespace: streaming }
spec:
  replicas: 3
  selector: { matchLabels: { app: connect } }
  strategy: { type: RollingUpdate, rollingUpdate: { maxUnavailable: 1, maxSurge: 0 } }
  template:
    metadata: { labels: { app: connect } }
    spec:
      hostNetwork: true                      # LAB ONLY — see §13.3
      dnsPolicy: ClusterFirstWithHostNet     # LAB ONLY — keeps cluster DNS with hostNetwork
      terminationGracePeriodSeconds: 60
      nodeSelector: { pool: connect }
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: DoNotSchedule
          labelSelector: { matchLabels: { app: connect } }
      containers:
        - name: connect
          image: lab-connect:local           # kind load docker-image lab-connect:local --name lab
          imagePullPolicy: Never
          envFrom: [{ secretRef: { name: connect-secrets } }]   # CONNECT_BOOTSTRAP_SERVERS etc.
          env:
            - { name: POD_IP, valueFrom: { fieldRef: { fieldPath: status.podIP } } }
            - { name: CONNECT_REST_ADVERTISED_HOST_NAME, value: "$(POD_IP)" }
            - { name: CONNECT_GROUP_ID, value: lab-connect }
            - { name: CONNECT_PLUGIN_PATH, value: "/usr/share/java,/usr/share/confluent-hub-components" }  # QUOTES! see Appendix E
            # … the same CONNECT_* settings as connect-vm in §6.4 …
          resources:
            requests: { cpu: 250m, memory: 1Gi }
            limits: { memory: 1536Mi }
          readinessProbe:
            httpGet: { path: /connectors, port: 8083 }
            initialDelaySeconds: 20
            periodSeconds: 5
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: { name: connect, namespace: streaming }
spec:
  minAvailable: 2
  selector: { matchLabels: { app: connect } }
```

⚠ **Gotcha found while testing:** in flow-style YAML (`{ name: X, value: a,b }`) a comma
splits the value. Without quotes, `CONNECT_PLUGIN_PATH` became just `/usr/share/java`, the
pods couldn't find Debezium or the sink, and every task FAILED with *"not a recognizable
type (source or sink)"*. Always quote values containing commas.

Do the same for Schema Registry (`k8s/schema-registry.yaml`, 2 replicas,
`SCHEMA_REGISTRY_HOST_NAME` from `status.podIP`, a Service on 8081).

### 12.3 Break it ✅ verified

| Drill | Result on this Mac |
|---|---|
| 3 replicas scheduled | one pod per zone (`lab-worker`, `lab-worker2`, `lab-worker3`) |
| `kubectl drain lab-worker` (zone-a) | allowed; pod rescheduled; connectors kept RUNNING |
| then `kubectl drain lab-worker2` | **blocked**: "Cannot evict pod as it would violate the pod's disruption budget" |
| row counts after drains | 1110 = 1110 |

Also try: `kubectl delete pod <one connect pod>` (graceful) and
`docker kill lab-worker3` (a whole "zone" crashes). Time each with the writer running.

**Checkpoint**
- [ ] Why is the advertised host the pod IP and not a Service name?
- [ ] Why no autoscaling for Connect?
- [ ] What does Cloud NAT give us that pod IPs can't?
- [ ] A zone dies: how many workers remain, and what do they do?

---

## Phase 13 — Moving safely: VM → Kubernetes

**Goal:** move live connectors from the "VM" (compose) to Kubernetes with **zero data loss**
and no full stop, then roll back once for practice.
**Time:** 5 hours. Plan page: **Moving safely**.

### 13.1 The three rules

1. **Move, never recreate.** Same connector names, same slot names, same topics, same group
   id. A connector with the same name finds its bookmark; a new name starts from zero.
2. **Safety net first.** Backups (Phase 10), alerts (Phase 15), retention ≥ 7 days, topic
   trimming paused — *before* step 1.
3. **Count after every step.** `count-check.sh` must say OK before the next step.

### 13.2 The three stages

```
STAGE 1 · TODAY           STAGE 2 · SIDE BY SIDE              STAGE 3 · DONE
[VM worker]          →    [VM worker] + [GKE 1] (+2, +3)  →   [GKE 1][GKE 2][GKE 3]
all connectors            same group id, connectors spread    VM stopped, kept 2 weeks for rollback
```

### 13.3 Networking — the part that bites ✅ verified

Every worker must reach every other worker's **advertised host on 8083** (requests are
forwarded to the leader). In the lab:

1. Put the compose containers on kind's Docker network:
   `docker network connect --alias kafka-1 kind lab-kafka-1-1` (repeat for every service), or
   in compose: `networks: { default: { name: kind, external: true } }`.
2. Pods → compose: works. A normal pod resolved and reached `kafka-1:9092` and
   `connect-vm:8083`.
3. Compose → pod IP (`10.244.x.x`): **does not work** — a plain container on the kind network
   timed out reaching a pod IP. So in the lab the Connect pods use `hostNetwork: true`; their
   "pod IP" is then the node IP (`172.18.x.x`), which compose can reach.

**In GKE** (VPC-native cluster) pod IPs are routable inside the VPC, so the VM can reach
pod IPs directly if the firewall allows TCP 8083. And the VM must stop advertising `connect`
(a name only its own Docker network knows) — set `CONNECT_REST_ADVERTISED_HOST_NAME` to the
**VM's internal IP**. Without this, any REST call that has to be forwarded to the VM (when the
VM is leader) fails.

### 13.4 Build — the drill ✅ verified

1. Writer running. `count-check.sh` = OK.
2. Deploy `connect` with `replicas: 1` (same `CONNECT_GROUP_ID: lab-connect`).
3. Watch: the pod joined, the **sink moved to the pod**, and the pod became the group
   **leader** (`grep leaderUrl` in its logs).
4. Prove forwarding: create a connector through the **VM** (`localhost:8083`) while the pod is
   leader → succeeded (forwarded to `172.18.0.3:8083`). Delete it.
5. Scale to 3. `docker stop -t 60 lab-connect-vm-1` (graceful). All tasks on pods.
6. `count-check.sh` → 1110 = 1110.

### 13.5 Break it

- **Rollback:** `docker start lab-connect-vm-1`, scale the Deployment to 0. Everything returns
  to the VM. Counts still match.
- **Crash during the move:** `docker kill` the VM worker while connectors are spread.
  Measure how long until its tasks move (≈ session timeout + 30 s).

### 13.6 The production step list

| # | Step | CDC stream | Console | Way back |
|---|---|---|---|---|
| 1 | Safety net: backups, DB-free alerts, 7+ day retention, pause topic trim | no pause | up | turn off |
| 2 | Cloud NAT static IP → allowlists (source DBs, Kafka, ClickHouse); test from one pod | no pause | up | nothing to undo |
| 3 | 2 Schema Registry pods join the same registry (same `_schemas`, same ids) | no pause | up | stop new pods |
| 4 | 1 Connect pod joins; watch a day; then 3; then stop the VM gracefully | seconds per connector | up | start VM worker |
| 5 | Metadata DB: nothing to do (already Cloud SQL HA + PITR) | — | — | — |
| 6 | Watchdog: report-only first, then allowed to restart | no pause | up | turn off |
| 7 | Extractor + Temporal: one sync/job type at a time behind a flag | no pause | up | flip flag |
| 8 | More writers: one sync at a time | seconds per sync | up | lower again |
| 9 | Final counts, topic trim back on, retire VM after 2 quiet weeks | no pause | up | — |

**Checkpoint**
- [ ] Why does keeping the same connector name matter so much?
- [ ] What breaks if the VM advertises `connect` while a GKE pod needs to forward to it?
- [ ] Why start with **one** GKE worker and watch a day?
- [ ] What's the rollback at step 4, and does it lose data?

---

## Phase 14 — Metadata DB: Cloud SQL HA + app hardening

**Goal:** understand what Cloud SQL HA does for us, and make the API behave well during the
~60 s failover.
**Time:** 5 hours. Plan page: **Upgrade 3** (HA part already done in prod).

### 14.1 Concepts

- **Cloud SQL HA (regional)** — a standby copy in another zone, kept in sync. If the primary's
  zone fails, the standby takes over. Google: the instance is unavailable for **about 60
  seconds**, the **IP stays the same**, and **all existing connections are closed** — clients
  must reconnect.
- **PITR (point-in-time recovery)** — restore the DB to any second within the retention window.
  Protects against *mistakes* (bad migration, wrong DELETE), which HA does not: HA copies the
  mistake to the standby instantly.
- **Our prod instance** (checked 2026-11-13): `production-solutions-dashboard-service-instance`,
  HA regional, PITR on, Postgres 17, shared with another service.
- **App hardening** — HA only helps if the app survives the 60 s:
  1. **Retry short blips** with backoff + jitter (e.g. 200 ms, 400 ms, 800 ms… up to ~2 min).
  2. **Skip a cycle** — a poller that can't reach the DB logs and waits for the next tick;
     it doesn't crash or spin.
  3. **`/ready` returns 503** while the DB is unreachable, so Kubernetes stops sending traffic
     to that pod (but doesn't kill it — that's liveness, keep it DB-free).
  4. **Never trim or delete when unsure** — if you can't read state, do nothing destructive.
- **Why not Redis as a backup copy?** Two copies of state can disagree, and our jobs rely on
  Postgres row locks (`ClaimAutoRestart`, job claims) Redis can't give safely.

🏭 **In our code:** `/ready` is wired in `internal/router/router.go:147`;
`DB_CONNECT_RETRY_TIMEOUT=2m` and `DB_MAX_CONNS` in config; pollers in `internal/handlers`.

### 14.2 Build — `cmd/metaapi`

A tiny Go service on `:8090`:

- `pgxpool` to `meta-db` (`localhost:5434`).
- `GET /live` → always 200 (process is alive).
- `GET /ready` → `pool.Ping(ctx)` with a 1 s timeout → 200 or 503.
- A poller every 10 s: `SELECT count(*) FROM schema_backups` with retry/backoff; on failure
  after ~5 s, **log "skipping cycle"** and wait for the next tick.
- Log every state change ("db down", "db back") once — not every attempt.

### 14.3 Break it

1. `docker compose stop meta-db` → `/ready` = 503, `/live` = 200, poller logs "skipping
   cycle", process keeps running.
2. `docker compose start meta-db` → recovers by itself, no restart.
3. `docker compose restart meta-db` while hammering an endpoint that writes → no request hangs
   forever (every query has a timeout).
4. Simulate the HA failover gap: `docker pause meta-db` for 60 s, then `unpause`. Watch
   connections being re-made.

**Second scenario:** the Schema Registry or ClickHouse is briefly down during a schema apply.
Same pattern: retry, skip, never half-apply.

**Checkpoint**
- [ ] HA vs PITR — which one saves you from a bad `DELETE`?
- [ ] Why must `/live` *not* check the DB?
- [ ] What does "skip a cycle" protect against?
- [ ] During a 60 s failover, does the CDC stream stop? (Hint: Phase 15)

---

## Phase 15 — Streaming without the metadata DB

**Goal:** if the metadata DB is down, Postgres CDC keeps flowing, failed connectors still get
restarted, and alerts still fire.
**Time:** 8 hours. Plan page: **3b**.

### 15.1 What already works and what doesn't

- ✅ Connect, Debezium, Kafka, Schema Registry and the sink **don't use the metadata DB**. The
  stream keeps going while it's down.
- ❌ Our auto-restart **does** need it: `autoRestart` (`internal/handlers/health_alerts.go:381`)
  calls `ClaimAutoRestart` (`internal/metadata/auto_restart_store.go:31`), and the poller lists
  syncs from the DB. DB down + connector fails = **stays failed until the DB is back**.
- ❌ Slack alerts are sent from the same DB-backed poller.

### 15.2 Concepts

- **Watchdog** — a small program next to Connect that only talks to Connect's REST API.
  Every 30 s: list connectors; for FAILED connectors/tasks, restart with
  `POST …/restart?includeTasks=true&onlyFailed=true`; **backoff 1 / 5 / 15 min** per connector
  (in memory); after 3 failed tries stop and alert; **skip PAUSED/STOPPED** (a human did that).
- **Debezium `errors.max.retries`** — Debezium itself restarts on *retriable* errors (DB
  connection lost). The watchdog covers everything else (sink failures, non-retriable errors
  that later become fixable).
- **Prometheus** — scrapes `/metrics` from your programs every N seconds and stores numbers
  over time. **Alert rules** fire when a number stays bad for a while (`for: 2m`).
- **The 4 alerts that need no metadata DB:**

| Alert | Signal | Why |
|---|---|---|
| Task failed | count of FAILED tasks > 0 for 2 min | a stopped stream |
| Sink lag | consumer group `connect-<sink>` lag growing for 10 min | data late (also catches "RUNNING but dead" from §9.4) |
| Source slot lag | `pg_wal_lsn_diff(current, confirmed_flush_lsn)` > threshold | WAL filling the customer's disk |
| Metadata DB down | `metaapi /ready` failing for 2 min | console/jobs down |

### 15.3 Build — `cmd/guardian`

One Go binary, no metadata DB:

- **Watchdog loop** (above). Start with `-dry-run` (report only) — this is step 6 of the move.
- **Metrics** with `prometheus/client_golang` on `:9100/metrics`:
  - `connect_tasks_failed{connector}` (gauge)
  - `guardian_restarts_total{connector,result}` (counter)
  - `source_slot_lag_bytes{slot}` — query the **source** DB (read-only user)
  - `sink_consumer_lag{connector}` — `kadm.NewClient(kgoClient).Lag(ctx, "connect-orders-sink")`,
    then `.Lag.Total()` on the group result (check with `go doc github.com/twmb/franz-go/pkg/kadm Client.Lag`)
- Log one line per action: `restart orders-sink attempt=2 next_in=5m`.

`monitoring/prometheus.yml` scrapes `guardian` and `metaapi`; `monitoring/alerts.yml` holds
the 4 rules. Add `prom/prometheus` to compose (port 9090). Grafana is optional.

### 15.4 Break it — the drill

1. Stop `meta-db`. Run the writer. Counts still increase in ClickHouse.
2. While `meta-db` is down: `RENAME` the ClickHouse table (§9.5) → task FAILED. Rename it back.
   **Guardian** restarts it within its backoff; the missing rows arrive.
3. Keep it broken: guardian tries at 1, 5, 15 min, then stops and the alert fires in
   Prometheus (`localhost:9090/alerts`).
4. `docker kill` a Connect worker → watch `sink_consumer_lag` rise *while status still says
   RUNNING*, then fall when the task moves.
5. `PUT …/pause` a connector → guardian must **not** touch it.

**Checkpoint**
- [ ] Why can't the current auto-restart work while the DB is down?
- [ ] Why does the watchdog skip PAUSED connectors?
- [ ] Which alert catches a dead worker whose tasks still show RUNNING?
- [ ] Why backoff instead of restarting every 30 s forever?

---

## Phase 16 — Temporal for jobs

**Goal:** run an extraction run as a durable Temporal workflow that survives a worker crash
mid-run.
**Time:** 8 hours. Plan page: **Upgrade 4**. Learn Temporal itself in `../temporal-poc`
(Phases 1–6 there) first — this phase only applies it.

### 16.1 What moves to Temporal, and what doesn't

| Move to Temporal (multi-step, long, must resume) | Keep as simple pollers |
|---|---|
| Extraction runs (first) | sync events / health polling |
| Backfills | usage counters |
| Re-partition (copy → verify → swap) | DB monitoring (`pg_monitor`) |
| Create / rollback of a sync | topic trimming |
| DLQ replay, Slack alert delivery | |

Rule: if it has **steps that must not repeat or be skipped after a crash**, it's a workflow.

### 16.2 Temporal needs its own database

Temporal stores workflow history in its **own** DB (Postgres/MySQL/Cassandra), not in our
metadata DB. Options: **Temporal Cloud** (no DB to run) or self-host with its own HA Postgres.
If Temporal's DB is down, workflows pause and continue later — they don't lose their place.
(Not yet tested by us: Temporal resuming after its DB fails over — add it to E2-style drills.)

### 16.3 Build — `cmd/jobs`

Run the Temporal dev server **in Docker**, next to the rest of the lab ✅ verified. Add to
`docker-compose.yml`:

```yaml
  temporal:
    image: temporalio/temporal:1.8.2
    command: ["server", "start-dev", "--ip", "0.0.0.0", "--db-filename", "/home/temporal/temporal.db"]
    ports: ["7233:7233", "8233:8233"]          # 7233 = gRPC for workers/CLI, 8233 = Web UI
    volumes: ["temporal-data:/home/temporal"]   # keeps workflows across restarts

volumes:
  temporal-data:
```

Why mount at `/home/temporal`: the image runs as user `temporal` (uid 1000). A volume mounted
at a new path like `/data` is owned by root, and the server fails with *"unable to open
database file"*. `/home/temporal` already exists and belongs to that user.

Without `--db-filename` the dev server keeps everything **in memory** and a restart forgets
every workflow. With it, a running workflow survived `docker compose restart temporal` on this
Mac. Check: `temporal workflow list --address localhost:7233`, UI at `http://localhost:8233`.

Your Go worker and starter connect to `localhost:7233` from the Mac, or `temporal:7233` from
a container.

`ExtractionRunWorkflow(ctx, run RunInput)`:

1. Activity `MarkRunStarted` → writes `runs` row in meta-db (idempotent on `run_id`).
2. Activity `RunExtractor` → runs the extractor (Phase 17 makes this a Kubernetes Job), long
   `StartToCloseTimeout`, **heartbeats** progress (`activity.RecordHeartbeat(ctx, rowsDone)`).
3. Activity `VerifyCounts` → compares rows written vs Kafka offsets.
4. Activity `MarkRunDone`.

Use `run_id` as the **workflow ID** so the same run can't be started twice.

### 16.4 Break it

- Kill the Temporal worker during step 2 → restart it → the workflow continues at step 2
  (heartbeat details tell it where it was). Read the Event History in the UI (`:8233`).
- **Temporal's own DB goes away:** `docker compose stop temporal` during step 2, wait a minute,
  `start` it. The workflow continues from where it was (the SQLite file kept its history).
  This is the local version of "Temporal's DB fails over".
- Stop `meta-db` during step 1 → activity retries until it's back. No duplicate `runs` row.

**Checkpoint**
- [ ] Why use `run_id` as the workflow ID?
- [ ] What do heartbeats give a 2-hour activity?
- [ ] Why should Temporal not share our metadata DB?
- [ ] Why does topic trimming stay a simple poller?

---

## Phase 17 — Extractor as Kubernetes Jobs

**Goal:** run extractions in parallel as Kubernetes Jobs instead of one at a time on the VM.
**Time:** 6 hours. Plan page: **Upgrade 5**.

### 17.1 Concepts

- Today: `APP_ROLE=extractor` on the VM, launches connector containers through the **mounted
  Docker socket**, `EXTRACTOR_MAX_CONCURRENT=1` → a 2-hour GCS run blocks Ads runs.
- **Docker socket risk** — mounting `docker.sock` gives a container root-level control of the
  host. Jobs remove that need.
- **Job fields that matter:** `backoffLimit` (retries), `activeDeadlineSeconds` (hard time
  limit), `ttlSecondsAfterFinished` (auto-clean), resources (each run gets its own CPU/memory).

🏭 **In our code:** `internal/extract`, `internal/airbyte`; limits in `internal/config/config.go`.

### 17.2 Build

1. `cmd/extractor`: a fake extractor — reads JSON lines from a folder (mounted ConfigMap) and
   produces to Kafka with key = row id, then exits 0. Build image `lab-extractor:local`,
   `kind load docker-image`.
2. In `RunExtractor` (Phase 16) create a Job with `k8s.io/client-go`, then poll its status,
   heartbeating each poll. Name the Job after `run_id` → creating it twice fails with
   AlreadyExists, which you treat as "already running" (idempotent).
3. Start 3 runs at once → 3 Jobs in parallel.

### 17.3 Break it

- `kubectl delete pod` of a running Job → Job retries (`backoffLimit`); workflow keeps waiting.
- Kill the Temporal worker while a Job runs → on restart the activity finds the existing Job
  instead of starting a second one.

**Checkpoint**
- [ ] Why is naming the Job after `run_id` important?
- [ ] What does `activeDeadlineSeconds` protect against?
- [ ] Why is dropping `docker.sock` a security win?

---

## Phase 18 — Faster writers and bad-table isolation

**Goal:** more sink throughput for big topics, and one bad table can't stop the others.
**Time:** 5 hours. Plan page: **Upgrade 6**.

### 18.1 Concepts

- **`tasks.max` on the sink** — each task takes some partitions. Useful up to the partition
  count; beyond that, extra tasks sit idle. Order is still kept per key (same partition).
- **One sink per big table** — isolates failures and lets each scale on its own.
- **`errors.tolerance=none`** (today) — a bad message FAILS the task → every table in that sink
  stops. Safe (nothing skipped) but wide blast radius.
- **`errors.tolerance=all` + `errors.deadletterqueue.topic.name`** — bad batches go to a DLQ
  topic and the rest continue. Must be paired with a DLQ alert + replay, or rows silently go
  missing.

🏭 **In our code:** sink tasks = 1 and `errors.tolerance: none` today; DLQ retry lives in
`internal/dlqretry`.

### 18.2 Build

1. Recreate `lab.public.orders` with 6 partitions (new topic name, e.g. via a new
   `topic.prefix`). Run the writer at a high rate. Measure sink lag with `tasks.max` 1 → 3 → 6.
2. Add a second table `payments` with its own sink. Break `orders` (§9.5) → `payments` keeps
   flowing.
3. Try DLQ on a test sink: produce a message that can't be inserted, see it land in the DLQ
   topic, the task stays RUNNING, then replay it after fixing the table.

**Checkpoint**
- [ ] Why does `tasks.max=10` on a 6-partition topic not help?
- [ ] Does more tasks break per-row ordering? Why not?
- [ ] What must exist before you turn on `errors.tolerance=all`?

---

## Phase 19 — Capstone: game day

**Goal:** run the plan page's "What if it breaks?" table for real, with numbers.
**Time:** 4 hours.

With the writer running and everything from Phases 10–18 deployed, run each and fill in
**measured** time-to-recover and the `count-check.sh` result:

| Failure | How to cause it | Target |
|---|---|---|
| Streaming machine dies | `docker kill` a kind node running Connect | moves in ~30–50 s, counts OK |
| A whole zone goes down | `docker kill` one kind node + `docker compose stop kafka-X` | other 2 zones carry on |
| One Kafka address down | stop the broker listed first in bootstrap | no effect |
| Metadata DB down | `docker compose stop meta-db` | stream continues, `/ready` 503 |
| Metadata DB down + connector fails | both at once | guardian restarts it |
| Pod crashes mid-job | kill the Temporal worker during an extraction | continues same step |
| One big extraction | start 1 long + 2 short runs | short ones finish first |
| Planned upgrade | `kubectl rollout restart deploy/connect` | no downtime, counts OK |

Then write a one-page **runbook** in `NOTES.md`: for each alert, the first 3 commands to run.

**Checkpoint**
- [ ] Every row has a measured number and a count result
- [ ] You can present the failure table with evidence, not guesses

---

# Electives

## E1 — MirrorMaker 2 standby cluster

A second Kafka cluster that copies the first, for disaster recovery.

- **Asynchronous** — it copies *after* the fact, so failing over can lose the last seconds
  (RPO > 0).
- **Offset translation is imperfect** — consumers may re-read or need care when switching.
- The Connect image already contains `MirrorSourceConnector` (you saw it in the plugin list).

**Build:** a single-broker "standby" cluster in compose; run MM2 as connectors; fail over the
sink to the standby and run `count-check.sh`. Note what was duplicated or missing.

## E2 — When the *source* Postgres fails over

Customer DBs can be HA too. A replication slot lives on the primary; after a failover the new
primary may not have it (depends on the provider and Postgres version — PG 17 has failover
slots, Cloud SQL has its own rules). Explore: what Debezium does when the slot is gone, and why
a fresh slot means a re-snapshot or a gap. Read the provider docs before trusting any answer.

---

# Appendices

## Appendix A — Glossary

| Word | Plain meaning |
|---|---|
| acks=all | producer waits until all in-sync copies have the message |
| Advertised host | address a server tells others to use to reach it |
| Avro | compact binary data format; schema stored separately |
| Bootstrap servers | addresses a Kafka client uses only to first find the cluster |
| CDC | change data capture — copying every row change as it happens |
| Consumer group | consumers sharing a name split a topic's partitions |
| Converter | Connect part that turns bytes ↔ rows |
| DLQ | dead letter queue — a side topic for messages that couldn't be written |
| ELR | eligible leader replicas — Kafka 4 list of copies safe to become leader (you'll see it in `--describe`) |
| FINAL | ClickHouse keyword: merge duplicates at query time |
| Heartbeat (Debezium) | periodic message so the slot keeps moving on quiet tables |
| HA | high availability — a standby takes over automatically |
| ISR | in-sync replicas — copies fully caught up with the leader |
| KRaft | Kafka's built-in voting for metadata; needs a majority of controllers |
| LSN | a position in the Postgres WAL |
| min.insync.replicas | minimum copies that must have a message for `acks=all` to succeed |
| Offset | position in a Kafka partition; also Connect's word for a bookmark |
| Partition | one ordered log inside a topic |
| PDB | PodDisruptionBudget — limits planned evictions |
| PITR | restore a DB to any point in time |
| Publication | Postgres list of tables a slot emits |
| Rebalance | redistributing connectors/tasks when workers join or leave |
| ReplacingMergeTree | ClickHouse engine that keeps the newest row per key |
| Replication slot | Postgres bookmark that holds WAL for a reader |
| RF | replication factor — number of copies |
| RPO / RTO | how much data you can lose / how long until it's back |
| Schema id | global number for one exact schema; written in every Avro message |
| Signal table | table you insert into to command Debezium (e.g. re-copy a table) |
| SMT | single message transform, e.g. `unwrap` |
| SPOF | single point of failure |
| Subject | named list of schema versions (`<topic>-value`) |
| Task | the thread inside a connector that copies data |
| Topology spread | Kubernetes rule to spread pods across zones |
| WAL | Postgres's write-ahead log — the diary of all changes |
| Watchdog | small program that restarts failed connectors, no DB needed |
| Worker | one Kafka Connect process |

## Appendix B — Verified on this Mac

Run on 2026-11-14 in a scratch copy of this lab (Docker Desktop 16 GB, Apple Silicon).

| Test | Result |
|---|---|
| 3-broker KRaft (Kafka 4.1.1), RF 3, racks a/b/c | partitions spread over 3 brokers, leaders spread |
| 1 broker down, `acks=all` | writes OK |
| 2-copy topic, 1 copy down, `acks=all` vs `acks=1` | `acks=all` refused, `acks=1` OK |
| 2 of 3 brokers down | even `--list` timed out (KRaft quorum lost) |
| bootstrap = only the dead broker vs list of 3 | single hangs; list works |
| 2 Schema Registries, leader stopped | write via the other succeeded (new leader) |
| Postgres → Debezium 3.6 → Kafka → sink 1.4.0 → ClickHouse 26.8 | 1000/1000 rows; update `__op=u`; delete `__deleted=true` |
| Offsets API: stop → PATCH sink to 0 → resume | raw rows 2001, `FINAL` 999 (dedup works) |
| PATCH offsets while RUNNING (empty body) | 400 error |
| 2nd worker joins | only the source moved (incremental rebalance) |
| `docker kill` worker under load | status stale ~40 s, moved ~45–50 s, counts 1059 = 1059 |
| `docker stop` (graceful) | moved in ~3 s |
| Sink table renamed | connector RUNNING, task FAILED; `restart?includeTasks&onlyFailed` → 202, recovered |
| Schema backup → empty registry in IMPORT mode | all 6 schemas kept their ids |
| kind 3 zones; pod → compose by name | works |
| compose → plain pod IP | timed out (hence `hostNetwork` in the lab) |
| Connect pod joins compose worker's group | sink moved to pod, pod became leader |
| REST call to VM forwarded to pod leader | worked (with `hostNetwork`) |
| Unquoted `CONNECT_PLUGIN_PATH` in flow YAML | plugins missing → tasks FAILED "not a recognizable type" |
| Phase 1 by hand: slot → JSON → Kafka → ClickHouse | WAL showed INSERT + UPDATE for id 42; ClickHouse `FINAL` = 42, paid |
| Temporal dev server in Docker (`temporalio/temporal:1.8.2`) with a volume at `/home/temporal` | UI 200; running workflow still listed after a container restart |
| 3 pods, drain 1 node, then drain 2nd | 1st allowed, 2nd blocked by PDB; counts 1110 = 1110 |

**Not yet verified** (do these yourself and add results here): Debezium `errors.max.retries`
behaviour on a real DB restart; WAL kept while Kafka is fully down; Temporal resuming after its
DB fails over; offsets restore by recreating a connector `STOPPED`; DLQ with the ClickHouse sink.

## Appendix C — Ports & URLs

| Service | From your Mac | From containers / pods |
|---|---|---|
| Kafka | `localhost:19092,29092,39092` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| Schema Registry | `localhost:8081`, `:8082` | `schema-registry-1:8081`, `-2:8081` |
| Connect (VM) | `localhost:8083` (`:8084` for connect-2) | `connect-vm:8083` |
| Source Postgres | `localhost:5433` (app/app, db shop) | `source-db:5432` |
| Metadata Postgres | `localhost:5434` | `meta-db:5432` |
| ClickHouse | `localhost:8123` (HTTP), `:9000` | `clickhouse:8123` |
| metaapi | `localhost:8090` | — |
| guardian metrics | `localhost:9100/metrics` | — |
| Prometheus | `localhost:9090` | `prometheus:9090` |
| Temporal (Docker) | gRPC `localhost:7233`, UI `localhost:8233` | `temporal:7233` |

## Appendix D — Command cheat sheet

```bash
# Kafka CLI (inside a broker)
k() { docker compose exec -T kafka-1 /opt/kafka/bin/$1 "${@:2}"; }
k kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic lab.public.orders
k kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group connect-orders-sink   # sink lag
# ⚠ In our REAL Connect container: always add -e KAFKA_HEAP_OPTS=-Xmx256m -e KAFKA_OPTS=
#    to docker exec, or the CLI's JVM can OOM-kill Connect.

# Connect
curl -s 'localhost:8083/connectors?expand=status' | jq -c 'to_entries[]|{n:.key,c:.value.status.connector.state,t:[.value.status.tasks[]|.state+"@"+.worker_id]}'
curl -X PUT  localhost:8083/connectors/NAME/stop          # pause | resume | stop
curl -X POST 'localhost:8083/connectors/NAME/restart?includeTasks=true&onlyFailed=true'
curl -s localhost:8083/connectors/NAME/offsets | jq

# Schema Registry
curl -s localhost:8081/subjects | jq
curl -s localhost:8081/subjects/lab.public.orders-value/versions/latest | jq '{id,version}'

# Postgres slot lag
psql postgres://app:app@localhost:5433/shop -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) lag FROM pg_replication_slots"

# ClickHouse
ch() { docker compose exec -T clickhouse clickhouse-client -u lab --password lab -d shop -q "$1"; }
ch "SELECT count() FROM orders FINAL WHERE __deleted='false'"

# Kubernetes (ALWAYS the lab context)
kubectl --context kind-lab -n streaming get pods -o wide
kubectl --context kind-lab -n streaming logs deploy/connect | grep -i leaderUrl
kind load docker-image lab-connect:local --name lab
```

## Appendix E — Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Tasks FAILED: *"not a recognizable type (source or sink)"* | plugin not on `plugin.path` (e.g. unquoted comma in flow YAML) | quote the value; check `plugin.path =` in the worker log |
| `curl: executable file not found` in Connect container | the cp-base image has no curl | call from the Mac or an `alpine` container |
| Connector RUNNING but no data | task FAILED, or worker dead and status stale | check tasks + sink lag, not just connector state |
| `NOT_ENOUGH_REPLICAS` | fewer than min ISR copies alive | bring brokers back; it's protecting you |
| Every Kafka command times out | KRaft quorum lost (2 of 3 down) | start brokers |
| Client hangs at start | its only bootstrap broker is down | list 3 brokers |
| Write via Schema Registry follower fails | follower can't reach leader's `HOST_NAME` | set a reachable host name / pod IP |
| REST create/update fails on one worker only | leader's advertised host unreachable from that worker | fix `CONNECT_REST_ADVERTISED_HOST_NAME` + firewall 8083 |
| PATCH offsets → 400 | connector not STOPPED, or empty body | `PUT …/stop` first |
| `cannot create logical replication slot in transaction that has performed writes` | slot created in the same transaction as a write | run it as its own statement |
| Temporal container: `unable to open database file` | volume mounted at a root-owned path | mount it at `/home/temporal` |
| Temporal forgot all workflows after restart | dev server without `--db-filename` = in-memory | add `--db-filename` + a volume |
| Slot lag keeps growing | connector stopped, or quiet tables on a busy DB | resume; enable heartbeat + action query |
| ClickHouse shows duplicates | normal before merges | query with `FINAL` |
| Sink FAILED after a schema change | ClickHouse table missing the new column | add the column, restart failed tasks |

## Appendix F — Progress tracker

| Phase | Title | Est. | Status | Finished |
|---|---|---|---|---|
| 0 | Environment & tooling | 1h | ☑ | 2026-11-16 |
| 1 | The big picture + pipeline by hand | 3h | ☐ | |
| 2 | Postgres WAL, slots, publications | 3h | ☐ | |
| 3 | Kafka basics | 3h | ☐ | |
| 4 | Kafka durability | 4h | ☐ | |
| 5 | Schema Registry and Avro | 3h | ☐ | |
| 6 | Kafka Connect | 4h | ☐ | |
| 7 | Debezium Postgres source | 4h | ☐ | |
| 8 | ClickHouse and the sink | 4h | ☐ | |
| 9 | Failure thinking and baseline drills | 3h | ☐ | |
| 10 | Kafka safety net | 6h | ☐ | |
| 11 | Kubernetes basics on kind | 5h | ☐ | |
| 12 | Connect and Schema Registry on Kubernetes | 6h | ☐ | |
| 13 | Moving safely: VM → Kubernetes | 5h | ☐ | |
| 14 | Metadata DB: HA + app hardening | 5h | ☐ | |
| 15 | Streaming without the metadata DB | 8h | ☐ | |
| 16 | Temporal for jobs | 8h | ☐ | |
| 17 | Extractor as Kubernetes Jobs | 6h | ☐ | |
| 18 | Faster writers and isolation | 5h | ☐ | |
| 19 | Capstone: game day | 4h | ☐ | |
| E1 | MirrorMaker 2 | 3h | ☐ | |
| E2 | Source Postgres failover | 2h | ☐ | |

## Appendix G — Plan page → phase map

| Plan page section | Phase(s) |
|---|---|
| Today vs target, red marks | 1, 9 |
| Upgrade 1 · GKE (3 Connect, 2 Schema Registry, PDB, spread, Cloud NAT) | 11, 12 |
| Upgrade 2 · Kafka (3 addresses, backups of `connect-offsets` + `_schemas`) | 4, 10 |
| Upgrade 3 · Metadata DB (Cloud SQL HA — already done) + app hardening | 14 |
| 3b · Stream without DB (watchdog, retries, DB-free alerts, retention, drill) | 7.1, 10.2, 15 |
| Upgrade 4 · Temporal | 16 |
| Upgrade 5 · Extractor as Jobs | 17 |
| Upgrade 6 · More writers, isolate bad tables | 18 |
| Moving safely (3 rules, 3 stages, 9 steps) | 13 |
| What if it breaks? / Why no data is lost | 1.3, 19 |
| Roadmap phases 1–5 | 10 → 12–13 → 14–15 → 16–17 → 18 |

## Appendix H — Real code map

| Topic | File |
|---|---|
| Connect worker settings (group, internal topics, acks, rebalance delay, advertised host) | `poc-debezium/docker-compose.yml` (`connect:` service) |
| Connect image + plugin versions/SHAs | `poc-debezium/Dockerfile` |
| Connector templates | `poc-debezium/connectors/*.json.template` |
| Debezium config built by the API | `cdc-etl-connect/internal/source/postgres/validator.go` |
| Connect REST client | `cdc-etl-connect/internal/connect` |
| ClickHouse table engine default | `cdc-etl-connect/internal/sink/clickhouse/table_layout.go` |
| Sink required settings | `cdc-etl-connect/internal/sink/clickhouse/trim_delivery.go` |
| Auto-restart (needs DB) | `cdc-etl-connect/internal/handlers/health_alerts.go:381`, `internal/metadata/auto_restart_store.go:31` |
| Reconcile connectors on sync load | `cdc-etl-connect/internal/handlers/sync_reconcile.go:210` |
| `/ready` route | `cdc-etl-connect/internal/router/router.go:147` |
| Topic retention / Kafka metrics | `cdc-etl-connect/internal/kafkametrics` |
| DB monitoring (slot lag alerts) | `cdc-etl-connect/internal/pgmonitor` |
| Extractor / Airbyte | `cdc-etl-connect/internal/extract`, `internal/airbyte` |
| DLQ retry | `cdc-etl-connect/internal/dlqretry` |

## Appendix I — Sources

- Kafka Connect REST API (offsets, stop, restart flags): https://kafka.apache.org/42/kafka-connect/user-guide/
- KIP-875 offsets API: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/231116066/KIP-875+First-class+offsets+support+in+Kafka+Connect
- KIP-745 restart failed tasks: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/181308623/KIP-745+Connect+API+to+restart+connector+and+tasks
- Debezium PostgreSQL connector (heartbeat, signals, `errors.max.retries`, `lsn.flush.mode`): https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- Schema Registry migration / IMPORT mode: https://docs.confluent.io/platform/current/schema-registry/installation/migrate.html
- Schema Registry leader election and HA: https://docs.confluent.io/platform/current/schema-registry/multidc.html
- Confluent multi-node Docker (KRaft): https://docs.confluent.io/platform/current/kafka/multi-node.html
- ClickHouse Kafka Connect sink: https://clickhouse.com/docs/en/integrations/kafka/clickhouse-kafka-connect-sink
- Cloud SQL HA (60 s, same IP, connections closed): https://docs.cloud.google.com/sql/docs/postgres/high-availability
- Cloud SQL enable HA: https://docs.cloud.google.com/sql/docs/postgres/configure-ha
- kind configuration: https://kind.sigs.k8s.io/docs/user/configuration/
- Kubernetes PDB: https://kubernetes.io/docs/tasks/run-application/configure-pdb/
- Kubernetes topology spread: https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/
- Temporal docs: https://docs.temporal.io
