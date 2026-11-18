# 3 — Replication slot commands (with the order 7 example)

ROADMAP §1.4 and Phase 2. All of these are **official, built-in Postgres functions** — the names
are fixed. Only the slot name (`by_hand` here) is your choice.

**What a slot is:** a replication slot is a named bookmark inside Postgres. It tells Postgres
"keep the WAL (write-ahead log, the change diary) from this position on, someone will read it."

## Setup used in the examples

Shortcut to run one SQL command inside the Postgres container (lives only in the current
terminal; paste it again in a new terminal, from the `p01-by-hand` folder):

```bash
p() { docker compose exec -T source-db psql -U app -d shop -tAc "$1"; }
```

The example data: order 7 goes `new` → `paid` → `shipped`.

```bash
p "CREATE TABLE orders (id bigint PRIMARY KEY, status text)"
p "INSERT INTO orders VALUES (7,'new')"
p "UPDATE orders SET status='paid' WHERE id=7"
p "UPDATE orders SET status='shipped' WHERE id=7"
```

## 1. Create a slot — `pg_create_logical_replication_slot`

```bash
p "SELECT pg_create_logical_replication_slot('by_hand','test_decoding')"
# → (by_hand,0/19763A8)
```

| Part | Meaning |
|---|---|
| `'by_hand'` | the slot's name — your choice (lowercase letters, numbers, underscores) |
| `'test_decoding'` | the output plugin (the translator that turns WAL lines into readable output). `test_decoding` = plain text, for learning. `pgoutput` = the one Debezium uses |
| result `by_hand` | the name of the slot that was created |
| result `0/19763A8` | the LSN (log sequence number, the position in the WAL) where the slot starts |

- Create the slot **before** making the changes you want to read. A slot only sees changes made
  after it was created.
- A slot can't be created in a transaction that has already written something, so run it as its
  own command (not in the same `psql -c` as a `CREATE TABLE`).

## 2. Read and move the bookmark — `pg_logical_slot_get_changes`

```bash
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

- `BEGIN 752` … `COMMIT 752` = one transaction (a group of changes that succeed or fail together); 752 is its number.
- The middle line = the change: table, kind of change, and each column with its type and new value.
- **It moves the bookmark.** Run the same command again and it prints nothing — Postgres now
  believes those changes were delivered and may clean up that part of the WAL.
- So: only "get" changes when you have really saved them somewhere else.

### The three arguments, and what `NULL` means

```
pg_logical_slot_get_changes( slot_name , upto_lsn , upto_nchanges )
```

| Argument | Meaning | `NULL` means |
|---|---|---|
| `slot_name` | which slot (bookmark) to read from | — (required) |
| `upto_lsn` | stop at this WAL position | no limit — read to the end |
| `upto_nchanges` | stop after about this many output lines | no limit — read everything waiting |

Examples:

```bash
# read everything that is waiting
p "SELECT data FROM pg_logical_slot_get_changes('by_hand', NULL, NULL)"

# read only about 4 lines
p "SELECT data FROM pg_logical_slot_get_changes('by_hand', NULL, 4)"

# read only up to a WAL position
p "SELECT data FROM pg_logical_slot_get_changes('by_hand', '0/1A00000', NULL)"
```

- **Catch:** Postgres only stops at the **end of a transaction**, never in the middle. So you may
  get a few more lines than you asked for. `BEGIN` and `COMMIT` lines count too.
- The function returns 3 columns: `lsn` (position), `xid` (transaction number) and `data` (the
  text). `SELECT data …` shows only the text; `SELECT * …` shows all three.

### How Debezium reads a big backlog (for example 1 GB)

Debezium does **not** use this SQL function. It opens a **streaming replication connection**
(a long-lived connection where Postgres keeps sending changes as a flow) and controls the pace:

- small batches: `max.batch.size` (default 2048 changes)
- a limited waiting line in memory: `max.queue.size` (default 8192 changes)
- when the line is full, Debezium stops reading and Postgres waits; unread changes stay in the WAL
- the bookmark moves forward bit by bit, as Kafka confirms each batch

## 3. Read without moving the bookmark — `pg_logical_slot_peek_changes`

```bash
p "SELECT data FROM pg_logical_slot_peek_changes('by_hand', NULL, NULL)"
```

- Same arguments and the same `NULL` meanings as `get_changes`.
- **It does not move the bookmark.** Run it 10 times and you get the same lines 10 times.
- Use it to look at what is waiting without consuming it.

| | `get_changes` | `peek_changes` |
|---|---|---|
| Shows the waiting changes | yes | yes |
| Moves the bookmark | **yes** | no |
| Run it twice | second time is empty | same output again |

## 4. List all slots — `pg_replication_slots`

This is a system view (a read-only table that Postgres fills in for you).

```bash
p "SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn, wal_status FROM pg_replication_slots"
# → by_hand|test_decoding|f|0/19C0030|0/19C0068|reserved
```

| Column | Meaning |
|---|---|
| `slot_name` | the slot's name |
| `plugin` | the output plugin it uses |
| `active` | `t` = a reader is connected right now, `f` = nobody is reading |
| `restart_lsn` | the oldest WAL position Postgres must keep for this slot |
| `confirmed_flush_lsn` | the bookmark: the position the reader confirmed it has received |
| `wal_status` | `reserved` = fine · `extended` = holding extra WAL · `unreserved` = about to lose WAL · `lost` = slot is broken (invalidated) |

How much WAL a slot is holding back (its lag):

```bash
p "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"
# → by_hand|f|320 bytes
```

- `pg_current_wal_lsn()` = where the WAL is now. The difference to the bookmark = unread WAL.
- A slot with `active = f` and a growing lag is the dangerous one: it keeps WAL forever and can
  fill the disk.

## 5. Delete a slot — `pg_drop_replication_slot`

```bash
p "SELECT pg_drop_replication_slot('by_hand')"
```

- The slot must be idle (`active = f`), otherwise Postgres refuses.
- After the drop, Postgres is free to delete the WAL this slot was holding (at the next checkpoint).
- ⚠ **In the real system this means data loss:** changes the reader had not read yet are gone.
  A new slot starts at "now", so there is a silent gap. Restart the failed connector first; drop
  a slot only when you accept a full re-copy (re-snapshot).

## 6. Move the bookmark forward by hand — `pg_replication_slot_advance`

```bash
p "SELECT pg_replication_slot_advance('by_hand', '0/1A00000')"
```

- Moves the bookmark to the given LSN. **Forward only**, never back.
- Everything skipped is never read → data loss. Not a normal operation.

## Quick reference

| Command | What it does | Moves the bookmark? |
|---|---|---|
| `pg_create_logical_replication_slot('name','plugin')` | creates a slot | — |
| `pg_logical_slot_get_changes('name', NULL, NULL)` | reads waiting changes | **yes** |
| `pg_logical_slot_peek_changes('name', NULL, NULL)` | looks at waiting changes | no |
| `SELECT … FROM pg_replication_slots` | lists slots, their bookmarks and status | no |
| `pg_drop_replication_slot('name')` | deletes a slot (its unread WAL can be lost) | — |
| `pg_replication_slot_advance('name','lsn')` | jumps the bookmark forward (skips data) | **yes** |
