# Notes

One entry per concept you practised. Keep it short. Template:

```
### §N.N <concept>
- Goal:
- What I built / ran:
- Failure I tried:
- Expected vs observed:
- What the platform still has to handle:
- Where else this applies:
- Status: conceptual | built | verified
```

At the end of each phase, add: **what surprised me** and **what I got wrong**.

---

## Phase 0 — Environment & tooling

### §0.1 Docker
- Goal: run every lab service in containers (isolated boxes, each with its own files, network and ports).
- Built / ran: Docker Desktop with 12.5 GB / 8 CPUs; tools: kubectl, jq, psql, Go, kind v0.33.0, Temporal CLI 1.10.1.
- Failure tried: — (concept) Postgres 14 on the Mac + Postgres 17 in Docker, both on port 5432.
- Expected vs observed: no clash — the container's port was not published (not opened on the Mac). Publishing 5432 would clash, hence 5433 later.
- Status: verified

### §0.2 kubectl context safety
- Goal: never run course commands against a real cluster.
- Built / ran: `kubectl config get-contexts` (lists contexts = saved "which cluster + which user" entries).
- Expected vs observed: one Mac had none; the other had dev, staging and production GKE contexts — current = **production**.
- Platform still has to handle: plain `kubectl` hits the current context → always pass `--context kind-lab`, or unset current-context.
- Status: verified

**What surprised me:** my second Mac's default kubectl context was production.
**What I got wrong:** nothing — the question didn't say one Postgres ran on the Mac and the other in Docker.

## Phase 1 — The big picture

Detailed notes: `notes/1_overview_flow.md`, `notes/2_duplicates_and_newest_wins.md`,
`notes/3_replication_slot_commands.md`, `notes/4_practical_1.md`.

### §1.1 The pieces and their jobs
- Goal: explain the path of one change from Postgres to ClickHouse.
- Path: WAL (Postgres's change diary) → replication slot (bookmark in Postgres) → Debezium (reader) → Schema Registry (table shape → small id) → Kafka topic (channel per table, 3 copies) → ClickHouse sink (writer) → ClickHouse.
- Kafka Connect (the program that runs Debezium and the sink) has workers (copies of the same image) that share the connectors (copy jobs).
- Failure tried: — (concept) a FAILED source connector whose slot holds 40 GB of WAL.
- Expected vs observed: dropping the slot deletes the unread WAL; a new slot starts at "now" → silent gap. Safer: restart only that connector — `POST /connectors/<name>/restart?includeTasks=true&onlyFailed=true`.
- Platform still has to handle: the slowest slot decides how much WAL Postgres keeps for the whole database.
- Also applies to: one connector per database (one slot) vs one per table (many slots); one sink for many topics vs one sink per table.
- Status: conceptual

### §1.2 SPOF, RTO, RPO
- Goal: tell downtime (late rows) apart from data loss (missing rows).
- Terms: SPOF (one part whose crash stops a lot) · RTO (how long until it works again) · RPO (how much data may be lost; target 0).
- Scenario: Connect down 60 h, WAL growing 1 GB/h, 100 GB free.
- Expected vs observed: 60 GB of WAL kept, nothing lost, Debezium resumes from the slot. RTO = 60 h (bad), RPO = 0.
- Platform still has to handle: no slot limit → disk can fill and stop the payment DB. With `max_slot_wal_keep_size` → DB survives but the slot is invalidated (broken) and needs a re-snapshot (full re-copy). Use a limit + an alert on slot lag.
- Data is lost only when something deletes it before it is copied: slot dropped · slot invalidated · Kafka retention (keep time) expired · bookmark moved forward by hand.
- Status: conceptual

### §1.3 At-least-once + newest wins
- Goal: explain why a message delivered twice is safe.
- Example: order 7, `new` → `paid` → `shipped`.
- Debezium stamps each message with `__op` (kind of change) and `__ts_ms` (time stamp) through the `unwrap` SMT (small edit on each message).
- Failure tried: sink crashes after writing a batch but before saving its consumer offset (its bookmark in Kafka) → the batch is written again = at-least-once delivery.
- Expected vs observed: extra rows stored, `SELECT … FINAL` still returns one row, `shipped`. Arrival order doesn't matter; highest `__ts_ms` wins.
- Why: `ReplacingMergeTree(__ts_ms) ORDER BY id` — sorting key `id` (same id = same row), version column `__ts_ms` (biggest wins).
- Platform still has to handle: readers must use `FINAL`; merges (background clean-up) run at an unknown time. The table mirrors the current state, not the history.
- Status: verified

### §1.4 Be the pipeline yourself
- Goal: do Debezium's and the sink's jobs by hand for one order.
- Built / ran (`p01-by-hand/`): slot `by_hand` with `test_decoding` (plain-text translator) → read 3 changes with `pg_logical_slot_get_changes` → typed 3 keyed messages into topic `orders` → piped the console consumer into ClickHouse (`ReplacingMergeTree(ver) ORDER BY id`).
- Failure tried: copied the topic into ClickHouse a second time (replay).
- Expected vs observed: `count()` 1 → 2, `FINAL` stayed `7 shipped 3`. Parts (chunks on disk) `all_1_1_1` + `all_2_2_1` → after `OPTIMIZE TABLE orders FINAL` one part `all_1_2_2`, `count()` 1.
- Platform still has to handle: I kept no bookmark. The real sink saves a consumer offset in `__consumer_offsets` after each batch; Kafka Connect runs both jobs non-stop in endless loops.
- Status: verified

**What surprised me:**
- `count()` was 1 after the first copy, not 3 — rows in the same insert batch are de-duplicated at once (`optimize_on_insert`); only rows in different batches wait for a merge.
- Reading a slot with `get_changes` empties it; reading a Kafka topic removes nothing.
- ClickHouse never updates a row in place — it inserts the new version and removes the old one at a merge.

**What I got wrong:**
- `FINAL` returns both `paid` and `shipped` for one id → no, one row per `ORDER BY` key.
- Kafka deletes a message after the sink reads it → no, it keeps it for the retention time.
- A slot size limit drops only the oldest WAL → no, it invalidates the whole slot.
  - Past `max_slot_wal_keep_size`, Postgres deletes the old WAL and marks the slot invalidated (`wal_status = lost`).
  - A slot promises "everything after my bookmark is still here"; a hole breaks that, so the slot can't be read at all — not even the WAL that is left.
  - Way back: re-snapshot (full re-copy of the table) with a new slot. It restores the latest value of every existing row, but not the steps in between, and not deletes (a deleted row stays in ClickHouse) → clean the destination table first.
  - Why not always snapshot: it reads the whole table every time (50 M rows for 3 changes), is hours old, loads the source DB and can't see deletes. Snapshot once at the start, then read the WAL (CDC = capture only the changes).
- Mixed up RTO with the time left before the disk fills. They are two different clocks:
  - RTO = how long the service was down → 60 h (Fri 8 pm → Mon 8 am). Tells how late ClickHouse is.
  - Safety margin = time before something worse happens → 100 GB ÷ 1 GB/h = 100 h until the disk is full. Spare time here: 100 − 60 = 40 h.
