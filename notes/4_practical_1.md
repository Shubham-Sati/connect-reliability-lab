# 4 — Practical 1: be the pipeline yourself (order 7 by hand)

ROADMAP §1.4. Folder: `p01-by-hand/`.

## What we want to achieve

Normally Debezium and the sink move the data. Here **you** do their jobs by hand, once, to see
exactly what each one automates.

- **Example:** order 7 goes `new` → `paid` → `shipped` in Postgres.
- **Goal:** at the end, ClickHouse shows one row: `7, shipped`.

| Step | What you do | Whose job you're doing |
|---|---|---|
| 1 | Start three empty boxes: Postgres, Kafka, ClickHouse | nobody's — setup |
| 2 | Create `orders`, make 3 changes, read them back from Postgres's diary | Debezium (reading) |
| 3 | Type those 3 changes into Kafka as messages | Debezium (writing) |
| 4 | Copy the messages from Kafka into ClickHouse | the sink |
| 5 | Copy them a second time on purpose; ClickHouse still shows one row | the crash and replay |

Keep **one terminal** open for the whole practical. The shortcuts `p`, `k` and `ch` live only in
that terminal — paste them again if you open a new one.

---

## Step 1 — Start three clean boxes

```bash
cd "/Users/shubhamsati/All Content/Learn/Learning Labs/connect-reliability-lab/p01-by-hand"
docker compose down -v      # throw away old containers and their data (-v = volumes too)
docker compose up -d        # start the three boxes in the background
docker compose ps           # check
```

Expected: three lines — `source-db`, `kafka`, `clickhouse` — each with status `Up`.

- The `PORTS` column shows `5432/tcp`, not `0.0.0.0:5432->5432`: the port is **not published**
  (not opened on the Mac), so this Postgres can't clash with one installed on the Mac.

## Step 2 — Make 3 changes in Postgres and read them from its diary (Debezium's reading job)

```bash
# shortcut: run one SQL command inside the Postgres box
p() { docker compose exec -T source-db psql -U app -d shop -tAc "$1"; }

# the source table
p "CREATE TABLE orders (id bigint PRIMARY KEY, status text)"

# place a bookmark (replication slot) in the diary (WAL)
p "SELECT pg_create_logical_replication_slot('by_hand','test_decoding')"

# the 3 changes to order 7
p "INSERT INTO orders VALUES (7,'new')"
p "UPDATE orders SET status='paid' WHERE id=7"
p "UPDATE orders SET status='shipped' WHERE id=7"

# read the diary through the bookmark  ← you are Debezium here
p "SELECT data FROM pg_logical_slot_get_changes('by_hand', NULL, NULL)"
```

Output:

```
BEGIN 752
table public.orders: INSERT: id[bigint]:7 status[text]:'new'
COMMIT 752
BEGIN 753
table public.orders: UPDATE: id[bigint]:7 status[text]:'paid'
COMMIT 753
BEGIN 754
table public.orders: UPDATE: id[bigint]:7 status[text]:'shipped'
COMMIT 754
```

- `BEGIN … COMMIT` = one transaction (a group of changes that succeed or fail together).
- The middle line = the change: table, kind of change, each column with its type and new value.
- The order is exactly the order typed. The diary never reorders.

**Experiment:** run the read command again → it prints **nothing**. `get_changes` reads **and
moves the bookmark**, so Postgres believes the changes were delivered. A reader must only move
the bookmark after the data is safely stored somewhere else.

More slot commands and the meaning of the `NULL`s: see `3_replication_slot_commands.md`.

## Step 3 — Put the 3 changes into Kafka as messages (Debezium's writing job)

```bash
# shortcut: run a Kafka tool inside the Kafka box
k() { docker compose exec -T kafka /opt/kafka/bin/$1 "${@:2}"; }

# create the topic (a named channel for one table)
k kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 1

# send 3 messages: key | value
printf '7|{"id":7,"status":"new","ver":1}\n7|{"id":7,"status":"paid","ver":2}\n7|{"id":7,"status":"shipped","ver":3}\n' | k kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders --property parse.key=true --property key.separator='|'

# read the topic back
k kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --property print.key=true --property print.offset=true --timeout-ms 6000
```

Output:

```
Offset:0	7	{"id":7,"status":"new","ver":1}
Offset:1	7	{"id":7,"status":"paid","ver":2}
Offset:2	7	{"id":7,"status":"shipped","ver":3}
ERROR Error processing message, terminating consumer process ... TimeoutException: null
Processed a total of 3 messages
```

- `--bootstrap-server localhost:9092` = the address of the Kafka broker (server) inside the box.
- **Key** (`7`) = which order the message is about. **Value** = the row itself.
- `ver` = the version number, written by hand. It plays the role of `__ts_ms`, which Debezium adds.
- **Offset** = the message's seat number in the topic.
- The `TimeoutException` is normal: the tool waited 6 s (`--timeout-ms 6000`), nothing new came, it stopped.
- The time in the log is UTC (5.5 hours behind India time).

**Experiment:** run the read command again → the **same 3 messages**. Reading Kafka removes
nothing; messages stay for the whole retention time. (Compare with the slot in step 2, where the
second read was empty.) This is what makes a replay possible.

## Step 4 — Copy the messages from Kafka into ClickHouse (the sink's job)

```bash
# shortcut: run one query inside the ClickHouse box
ch() { docker compose exec -T clickhouse clickhouse-client -u lab --password lab -d shop -q "$1"; }

# the destination table: one row per id, the biggest ver wins
ch "CREATE TABLE orders (id Int64, status String, ver Int64) ENGINE=ReplacingMergeTree(ver) ORDER BY id"

# copy Kafka → ClickHouse (takes ~6 s, prints nothing)
k kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --timeout-ms 6000 2>/dev/null | docker compose exec -T clickhouse clickhouse-client -u lab --password lab -d shop -q "INSERT INTO orders FORMAT JSONEachRow"

ch "SELECT count() FROM orders"       # → 1
ch "SELECT * FROM orders FINAL"       # → 7	shipped	3
```

- `ENGINE=` is a **ClickHouse** keyword (Postgres has none). It picks the table engine (table type).
- `ReplacingMergeTree(ver)` = keep the row with the biggest `ver`. `ORDER BY id` = rows with the same `id` compete.
- `ver` is an extra column: it exists in the Kafka message and the ClickHouse table, not in Postgres.
- `FORMAT JSONEachRow` = each incoming line is one row written as JSON.
- `2>/dev/null` hides the harmless timeout message.

**Why `count()` is 1 and not 3:** all 3 rows arrived in **one insert batch**. Before writing a
batch to disk, ClickHouse applies the engine's rule to that batch (setting `optimize_on_insert`,
on by default). So `ver 3` beat `ver 1` and `ver 2` at once.

- Rows in the **same** insert batch → de-duplicated immediately.
- Rows in **different** insert batches → each batch is its own **part** (a chunk of the table on
  disk); copies stay until a **merge** joins the parts.
- ClickHouse never updates a row in place. A later version is inserted as a new row; the old one
  is removed later by a merge. `FINAL` gives the right answer in the meantime.

## Step 5 — Copy the same messages again (the crash and replay)

```bash
# the same copy command as step 4, a second time
k kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --timeout-ms 6000 2>/dev/null | docker compose exec -T clickhouse clickhouse-client -u lab --password lab -d shop -q "INSERT INTO orders FORMAT JSONEachRow"

ch "SELECT count() FROM orders"                                              # → 2
ch "SELECT * FROM orders FINAL"                                              # → 7	shipped	3
ch "SELECT name, rows FROM system.parts WHERE table='orders' AND active"     # → all_1_1_1 1 / all_2_2_1 1

ch "OPTIMIZE TABLE orders FINAL"                                             # force the merge

ch "SELECT count() FROM orders"                                              # → 1
ch "SELECT name, rows FROM system.parts WHERE table='orders' AND active"     # → all_1_2_2 1
```

- `count()` = 2: the second copy was a new batch → a new part → a duplicate `7, shipped, 3`.
- `FINAL` = 1 row: the rule is applied across both parts while reading.
- `system.parts` = ClickHouse's own list of the parts each table is stored in.
- `OPTIMIZE TABLE … FINAL` forces the merge ClickHouse would otherwise run later by itself.
  Fine for learning; heavy on big tables, so real systems rely on `SELECT … FINAL`.

Reading a part name such as `all_1_2_2`:

| Piece | Meaning |
|---|---|
| `all` | the partition (group of parts); this table has only one |
| `1` | first insert batch inside this part |
| `2` | last insert batch inside this part |
| final `2` | level: how many rounds of clean-up the part has been through |

What this proved:

- A replay adds extra rows = **at-least-once delivery**.
- `FINAL` gives the right answer **before** any merge.
- The merge only tidies up afterwards.

## Clean up (when finished)

```bash
docker compose down -v
```

---

## The whole practical, with the technical names

| Step | What you did by hand | Who does it in the real system | Technical name |
|---|---|---|---|
| 1 | Started three boxes with `docker compose up` | Runs all the time on servers | **Containers** (Postgres, a Kafka **broker**, ClickHouse) |
| 2 | Placed a bookmark in Postgres's diary | Debezium, when the connector is first created | **Replication slot** on the **WAL** |
| 2 | Read the 3 changes with `pg_logical_slot_get_changes` | Debezium, continuously, as a flow | **Logical decoding**; `pgoutput` plugin over a **streaming replication** connection |
| 3 | Typed 3 messages with a key and a `ver` number | Debezium builds each message and adds `__op` and `__ts_ms` | **Source connector**; **SMT** `unwrap`; message **key** and **value** |
| 3 | Sent them to the `orders` topic | Debezium writes to `payment.public.orders` | **Topic** and **offset** |
| 4 | Piped the Kafka reader into a ClickHouse insert, once | The ClickHouse sink, in batches, forever, with retries | **Sink connector** |
| 4 | Created the table with `ReplacingMergeTree(ver) ORDER BY id` | Same, with `__ts_ms` as the version | **Table engine**, **version column**, **sorting key** |
| 5 | Copied the same messages a second time | Happens by itself after a crash | **At-least-once delivery** |
| 5 | Saw `count()` = 2 but `FINAL` = 1 row, then forced a merge | ClickHouse merges in the background; readers use `FINAL` | **Parts**, **merge**, **`FINAL`** |

**One line:** you were Debezium in steps 2 and 3, the sink in step 4, and the crash in step 5.
**Kafka Connect** is the program that runs both jobs for you, non-stop.
