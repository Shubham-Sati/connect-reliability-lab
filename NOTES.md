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
- Goal: run every lab service in isolated containers.
- What I built / ran: Docker Desktop with 12.5 GB / 8 CPUs; tools: kubectl, jq, psql, Go, kind v0.33.0, Temporal CLI 1.10.1.
- Failure I tried: — (concept) Mac Postgres 14 + Docker Postgres 17 at the same time.
- Expected vs observed: no clash — the container has its own IP; port 5432 was not published to the Mac. Publishing 5432 would clash (hence 5433 later).
- Status: verified

### §0.2 kubectl context safety
- Goal: never run course commands against a real cluster.
- What I built / ran: `kubectl config get-contexts`.
- Expected vs observed: one Mac had none; the other had dev, staging and production GKE contexts — current = **production**.
- What the platform still has to handle: plain `kubectl` hits whatever context is current → always pass `--context kind-lab` (or unset current-context).
- Status: verified

**What surprised me:** my second Mac's default kubectl context was production.
**What I got wrong:** thought the Homebrew Postgres was in a container too — it runs directly on the Mac; the real reason is the unpublished container port.

## Phase 1 — The big picture
