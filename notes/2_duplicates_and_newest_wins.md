# 2 — Duplicates and "newest wins" (the story of order 7)

ROADMAP §1.3. One example, followed from the source to the destination.

## What we want to achieve

The payment service has an `orders` table in Postgres. We want ClickHouse to always show the
**same latest status** for every order.

- **Example:** order 7 goes through 3 changes in Postgres: `new` → `paid` → `shipped`.
- **Goal:** ClickHouse shows exactly **one row** for order 7, with status **`shipped`**.
- **Problem:** on the way, messages can arrive **twice** or in the **wrong order**. The system
  must still end with the right answer.

## Step 1 — The changes happen in Postgres (the source)

The payment service runs three commands:

- 10:00:01 → `INSERT` order 7 with status `new`
- 10:00:02 → `UPDATE` order 7 to `paid`
- 10:00:03 → `UPDATE` order 7 to `shipped`

Postgres writes each one into the **WAL** (write-ahead log, its change diary), in this exact
order. The diary has 3 lines: `new`, `paid`, `shipped`. Nothing has left Postgres yet.

## Step 2 — Debezium picks up the changes and stamps each one

**Debezium** (the reader plugin running inside Kafka Connect) receives the 3 diary lines in the
same order. For each line it builds one message and adds two extra fields:

- `__op` (the kind of change): `c` = create/insert, `u` = update, `d` = delete, `r` = snapshot read
- `__ts_ms` (a time stamp in milliseconds): the moment Debezium handled this change

| # | Message | `__op` | `__ts_ms` |
|---|---|---|---|
| 1 | `id=7, status=new` | `c` | 1000 |
| 2 | `id=7, status=paid` | `u` | 2000 |
| 3 | `id=7, status=shipped` | `u` | 3000 |

- A **later change always gets a bigger stamp**, because Debezium reads the diary in order.
- So the stamp tells anyone, at any later time, which message is the newest.
- Real stamps are long numbers such as `1794752375874`; small numbers are used here to read easily.
- The fields are added by an **SMT** (single message transform, a small edit applied to each
  message on its way to Kafka) called **`unwrap`** (`ExtractNewRecordState`), with the setting
  `add.fields: op,ts_ms`.

## Step 3 — The 3 messages go into Kafka

Debezium sends them to the **topic** (a named channel for one table) `payment.public.orders`.
Kafka stores them in a line and gives each an **offset** (the message's seat number).

| Offset | Message | `__ts_ms` |
|---|---|---|
| 0 | `id=7, status=new` | 1000 |
| 1 | `id=7, status=paid` | 2000 |
| 2 | `id=7, status=shipped` | 3000 |

- **Kafka keeps all 3.** It never merges them. Every message stays until the **retention** time
  (for example 7 days) runs out.
- **The stamp travels with the message**, so any later reader still sees 1000, 2000, 3000.

## Step 4 — The sink copies to ClickHouse, and a crash creates duplicates

The **ClickHouse sink** (the writer plugin inside Kafka Connect) loops over 3 actions:

- **(a)** read a batch of messages from Kafka
- **(b)** write that batch into ClickHouse
- **(c)** save its bookmark (**consumer offset**): "I've finished up to offset 2"

What happens with our 3 messages:

- **(a)** The sink reads offsets 0, 1, 2.
- **(b)** It writes all 3 rows into ClickHouse. ✅
- **💥 The sink crashes** before (c). The bookmark was never saved.
- **Restart:** the bookmark still says offset 0, so the sink reads and writes the 3 rows **again**.

ClickHouse's raw table now holds **6 rows** for order 7:

| Row | status | `__ts_ms` |
|---|---|---|
| 1 | new | 1000 |
| 2 | paid | 2000 |
| 3 | shipped | 3000 |
| 4 | new | 1000 |
| 5 | paid | 2000 |
| 6 | shipped | 3000 |

- This is **at-least-once delivery**: every message arrived, some more than once. Nothing was lost.
- **Why not save the bookmark before writing?** A crash in between would then skip the batch
  forever = data loss. Duplicates can be cleaned up; missing rows can't.

## Step 5 — ClickHouse keeps only the newest row per order

The table was created with:

```sql
ENGINE = ReplacingMergeTree(__ts_ms) ORDER BY id
```

Two rules come from that line:

- **`ORDER BY id`** (the **sorting key**): rows with the same `id` are the same order, so only
  one survives.
- **`ReplacingMergeTree(__ts_ms)`** (the **version column**): among those rows, keep the one
  with the **biggest `__ts_ms`**.

Applied to the 6 rows:

1. **Group by id** — all 6 have `id=7`, so they compete.
2. **Compare stamps** — 1000, 2000, 3000, 1000, 2000, 3000. The biggest is 3000.
3. **Keep the winner** — `shipped (3000)`; its identical copy is dropped.
4. **Result** — 1 row: `7, shipped`. Same as Postgres. 🎯

When the cleanup happens:

- **Merge** (a background clean-up job): physically removes the losing rows, at a time you can't predict.
- **`FINAL`** (at query time): applies the rules right now, even if no merge has run.
  - `SELECT count() FROM orders WHERE id=7` → could be 6 (before a merge)
  - `SELECT * FROM orders FINAL WHERE id=7` → always 1 row, `shipped`

Things to remember:

- **Arrival order doesn't matter.** If the rows arrive as `shipped`, `new`, `paid`, the stamps are
  still 3000, 1000, 2000, and 3000 still wins. ClickHouse looks at the stamp on the row, never at
  when the row reached it.
- **No crash? Same result.** 3 rows go in (`new`, `paid`, `shipped`) → 1 row, `shipped`. The rule
  does two jobs: it keeps the **latest status** and it removes **duplicates**.
- **`new` and `paid` are gone from ClickHouse.** The table is a mirror of the **current state**,
  exactly like Postgres (an `UPDATE` overwrites the old status there too). It is not a history.
- **Where history still exists:** Kafka keeps all 3 messages until retention runs out. To keep
  history in ClickHouse you would need a different table design, for example
  `ORDER BY (id, __ts_ms)`.

## Step 6 — The whole story, with the technical names

| Step | What happened | Technical name |
|---|---|---|
| 1 | Postgres wrote the 3 changes to its diary, in order | **WAL** (write-ahead log) |
| 2 | Debezium turned each change into a message and added `__op` and `__ts_ms` | **SMT** `unwrap`; `__ts_ms` becomes the **version** |
| 3 | Kafka stored all 3 messages in a line, each with a seat number | **Topic** and **offset** |
| 4 | The sink wrote the rows, crashed before saving its bookmark, wrote them again | **At-least-once delivery**; bookmark = **consumer offset** |
| 5 | ClickHouse kept one row per `id`, the one with the biggest `__ts_ms` | **ReplacingMergeTree**; **sorting key** `id`, **version column** `__ts_ms`; **merge** or **`FINAL`** |

**One line:** at-least-once delivery + newest version wins = **effectively exactly once** (the
result looks as if every change arrived exactly once, even though some arrived twice).
