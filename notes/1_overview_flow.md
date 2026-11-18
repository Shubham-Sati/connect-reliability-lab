# Connect Service — Overview Flow

What the whole system does: copy every change (insert / update / delete) from a service's
Postgres table into ClickHouse, within seconds, forever — late is OK, missing is not.

## Diagram

```
 ┌──────────────────────────── SOURCE: payment service Postgres ────────────────────────────┐
 │                                                                                           │
 │  orders table ──INSERT/UPDATE/DELETE──▶ WAL (Write-Ahead Log = Postgres's change diary)   │
 │                                          │                                                │
 │   Replication slot (bookmark inside Postgres: "keep WAL from LSN X onwards")              │
 │   LSN (Log Sequence Number = position in the WAL, e.g. 0/19763A8)                         │
 │   Publication (list of tables the slot should send, e.g. orders, refunds)                 │
 └──────────────────────────────────────────┼────────────────────────────────────────────────┘
                                            │ Postgres PUSHES changes through the slot
                                            ▼
 ┌──────────── KAFKA CONNECT (the program that runs connectors + keeps bookmarks) ───────────┐
 │  Workers = copies of the same Connect image (today: 1 on the VM → later 3, same group.id) │
 │                                                                                           │
 │  ┌──────────────────────────────────────┐                                                 │
 │  │ Debezium = SOURCE connector          │──registers table shape──▶ Schema Registry       │
 │  │ (reads WAL, turns each change into   │◀──────── id 17 ──────────  (catalogue of table  │
 │  │  a message {id:42,status:'paid'})    │                             shapes → small ids) │
 │  └──────────────────┬───────────────────┘                                                 │
 └─────────────────────┼─────────────────────────────────────────────────────────────────────┘
                       │ message = key {id:42} + value {…, schema id 17}
                       ▼
 ┌──────────────────── KAFKA (durable message log) ──────────────────────────────────────────┐
 │  Topic (a named channel per table) e.g. payment.public.orders                             │
 │  Broker (one Kafka server) × 3, in 3 zones (separate data-centre buildings)               │
 │  Replication factor 3 = every message stored on 3 brokers                                 │
 │  Retention (how long messages are kept, e.g. 7 days) — NOT deleted when read              │
 │                                                                                           │
 │  Internal topics (Kafka's own storage for Connect's state):                               │
 │   • connect-offsets     (source bookmark: Debezium's last LSN)                            │
 │   • __consumer_offsets  (sink bookmark: "read up to message #N", group connect-<name>)    │
 │   • connect-configs     (connector configs)                                               │
 │   • connect-status      (who runs what: RUNNING / FAILED)                                 │
 └──────────────────────┬────────────────────────────────────────────────────────────────────┘
                        │
                        ▼
 ┌──────────── KAFKA CONNECT (same workers) ────────────┐
 │ ClickHouse sink = SINK connector                      │
 │ (reads Kafka, decodes with schema id 17,              │
 │  writes batches of thousands)                         │
 └──────────────────────┬────────────────────────────────┘
                        ▼
 ┌──────────── CLICKHOUSE (fast analytics DB) ──────────┐
 │ ReplacingMergeTree(__ts_ms) = keeps 1 row per id,     │
 │ the newest → duplicates collapse                      │
 └───────────────────────────────────────────────────────┘

 Around it:
   Go API (cdc-etl-connect) ──REST /connectors (port 8083)──▶ Kafka Connect   (create / pause / delete)
   Go API ──▶ Metadata DB (Cloud SQL Postgres = the API's memory: syncs, jobs, alerts — not customer rows)
   Extractor (runs Airbyte connectors = plugins for non-DB sources: Google Ads, Facebook Ads, GCS files)
             ──pulls on a schedule──▶ Kafka ──▶ sink ──▶ ClickHouse
   Today: Connect + Schema Registry + Extractor all on ONE VM (virtual machine) = SPOF
```

## The path of one change — step by step

1. **Write** — the payment service runs `UPDATE orders SET status='paid' WHERE id=42`.
   Postgres first writes it to the **WAL** (Write-Ahead Log, its change diary), then the table.
   Insert, update and delete all go to the WAL. A schema change (`ALTER`) is not sent as its
   own message — Debezium notices the new column on the next row change.
2. **Read** — Postgres **pushes** the change to **Debezium** through the **replication slot**
   (bookmark + connection). Only tables in the **publication** are sent.
3. **Shape** — Debezium registers the table's shape in **Schema Registry** and gets a small id
   (e.g. 17). The message carries only "shape 17", not the full column list. The registry also
   blocks breaking schema changes.
4. **Store** — Debezium sends the message to the topic `payment.public.orders`. It goes to one
   broker (the partition **leader**), which copies it to the other 2 → 3 copies.
5. **Confirm** — once Kafka has it, Debezium saves its LSN in `connect-offsets` and tells
   Postgres "safe up to LSN X" → the slot moves forward → Postgres may delete older WAL.
6. **Copy out** — the **ClickHouse sink** reads the topic, decodes using id 17, and writes
   batches into ClickHouse. Then it moves its own **consumer offset** (its bookmark).
7. **Keep newest** — if a message arrives twice, **ReplacingMergeTree(__ts_ms)** keeps one row
   per id: the newest. `SELECT … FINAL` shows the result.

**Kafka Connect** runs steps 2–6. Its **workers** (copies of the same image with the same
`group.id`) share the connectors; if one worker dies, the others take its jobs in ~45 s
(rebalance). All Connect state lives in Kafka, so any worker can continue any job.

## The 3 bookmarks

| Bookmark | Where | Moves when |
|---|---|---|
| Replication slot (`confirmed_flush_lsn`) | inside Postgres | Debezium confirms Kafka has the change |
| Source offset | Kafka topic `connect-offsets` | Debezium commits its last LSN |
| Sink consumer offset | Kafka topic `__consumer_offsets` (group `connect-<sink name>`) | ClickHouse accepted the batch |

## Connectors

- **Source connector** (Debezium): one per Postgres database is usual — one slot, reads the WAL
  in commit order, routes each table to its own topic. One per table also works, but each slot
  decodes the whole WAL again, and the **slowest slot** decides how much WAL Postgres keeps.
- **Sink connector**: one sink for many topics (simple), or one per table (one bad table can't
  block the others). Many sinks can read the same topic, each at its own offset.
- Names in this course: service + table, e.g. `payment-orders-source`, `payment-orders-sink`.

## Downtime vs data loss

- **SPOF** (Single Point Of Failure) — one part whose crash stops a lot. Today: the one VM.
- **RTO** (Recovery Time Objective) — how long until it works again.
- **RPO** (Recovery Point Objective) — how much data we may lose. Target: **0**.
- **Connect down = late, not lost** — changes wait in the WAL because the slot holds them.
- **Data is lost only when something deletes it before it's copied:**
  1. The slot is **dropped** → its WAL is deleted at the next checkpoint → a new slot starts at "now" → silent gap.
  2. The slot passes `max_slot_wal_keep_size` → slot **invalidated** → needs a full re-snapshot.
  3. Kafka **retention** expires before the sink reads (e.g. sink stuck 8 days, retention 7).
  4. A bookmark is moved **forward** by hand (`pg_replication_slot_advance`).
- **Trade-off on the slot size limit:** no limit → CDC safe but the disk can fill and stop the
  payment DB; with a limit → the DB survives but CDC breaks. Usually: set a limit + alert on slot lag.
- **First fix for a FAILED connector:** restart only that connector, not all of Connect:
  `POST /connectors/<name>/restart?includeTasks=true&onlyFailed=true`.
