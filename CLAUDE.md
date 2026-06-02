# Baby's First MapReduce

## Working Style

**The developer writes all code.** Do not write implementation code unless explicitly asked. Your role is to explain concepts, answer questions, review code when the developer shares it, and give feedback. If asked to implement something, explain how to do it instead and let the developer write it.

This project is a learning exercise — the developer is building expertise in Elixir and distributed systems. Doing the work for them defeats the purpose.

**Reviewing the code:** When the developer asks for feedback on their code, don't ask him to copy and paste the code into the session. Always try to locate the code yourself and read it before asking where it is. The implementation plan (referenced below) contains a list of files relevant to each task. If you know which task the developer is working on, you should easily be able to locate which files he is working.

---

## Current Status

The core system is implemented and has run a word-count job end-to-end across multiple physical machines. Working: map/reduce execution, the master scheduler, fault tolerance (`{:nodedown}` requeue, straggler backup tasks, stale-completion handling), locality simulation, the multi-machine launch path (`mix mr.worker` + `mix mr.start --distributed`), output collection on the master, and the Phoenix LiveView dashboard. Active work is refinement — code cleanup, documentation, and hardening surfaced during real multi-machine runs.

Key documents:
- **Multi-machine setup:** `docs/MULTI_MACHINE_SETUP.md` — end-to-end guide for a multi-machine run (node names, cookies, networking, troubleshooting).
- **Configuration:** `config/README.md` — the two run modes and the environment variables that drive them.
- **Original design spec:** `docs/superpowers/specs/2026-05-18-mapreduce-design.md` — the as-designed architecture and message protocol. A historical artifact scoped to the single-machine "phase 1"; some details (e.g. output collection) have since evolved, so cross-check against the code.
- **Multi-machine plan:** `docs/plans/multi-machine-parametrization-plan.md` — the task breakdown for the multi-machine parametrization work.

---

## Project Purpose

This project has two objectives, in priority order:

1. **Learning** — Hands-on experience designing a distributed system from first principles, using Elixir and the Erlang/OTP distributed computing model. Key learning targets: Elixir/OTP, distributed Erlang nodes, fault tolerance via supervision, the MapReduce execution model, and the shuffle/RPC pattern.
2. **Portfolio** — A talking piece for moving from application development into systems/distributed systems engineering. The implementation should be paper-faithful enough to discuss in a technical interview.

The reference paper is *MapReduce: Simplified Data Processing on Large Clusters* (Dean & Ghemawat, Google, OSDI 2004). All design decisions trace back to it. When in doubt, follow the paper.

---

## What We Are Building

A small but faithful implementation of the MapReduce programming model, running as a cluster of distributed Erlang nodes — on a single machine for development, or across several physical machines over a LAN with no code changes. The reference task is **word frequency count** across 20 large text files (`sample-data/`).

Both of the original "later iterations" are now done:
- Runs across multiple physical machines (connect Erlang nodes over the LAN; see `docs/MULTI_MACHINE_SETUP.md`)
- A Phoenix LiveView dashboard provides real-time visualisation

---

## Architecture

### Worker Isolation
Each worker (map or reduce) runs as a **separate Erlang node** — its own OS process. The master is also a separate node. Nodes connect over Erlang's built-in distribution protocol.

This means:
- Killing a worker is `kill -9 <pid>` or `System.stop()` on the node — the master detects it automatically via `Node.monitor_nodes(true)` and the `{:nodedown, node}` message.
- Extending to multiple machines requires no code changes — nodes connect by name over the network.

### RPC
Communication uses **Erlang distribution** — `GenServer.call({pid, node}, message)` for synchronous request/response, and `GenServer.cast` for async. No HTTP, no gRPC. This is OTP's native RPC.

### Execution Flow (following paper section 3.1)
1. Master splits input files into M chunks and assigns each to an idle map worker node.
2. Map worker reads its chunk, emits `{word, 1}` pairs, buckets them into R partitions using `hash(word) mod R`, and writes each partition to its **local disk** (`tmp/<worker-node-name>/map-<task-id>-bucket-<r>.bin`).
3. Map worker notifies master with file locations on completion.
4. Master assigns reduce tasks to idle reduce workers, passing them the locations of all bucket-R files across all map workers.
5. Reduce worker fetches each bucket file via RPC from the relevant map worker node, sorts all pairs by key, and runs the reduce function (sum counts per word).
6. Reduce worker sends its final output back to the master via RPC; the master's `OutputCollector` writes it to `output/` (one file per reduce bucket). This is a deliberate deviation from the paper (where reducers write straight to GFS) — it avoids needing a shared filesystem, at the cost of the master being an output funnel.
7. Master declares the job complete when all reduce tasks finish.

### Intermediate Data Storage
**Local disk + RPC fetch** (paper section 3.1, 3.3, 3.4). Map workers write to their own local `tmp/` directory. Reduce workers fetch via RPC. If a map worker dies before its data is fetched, those tasks are re-run on another worker — matching the paper's fault tolerance model exactly.

### Locality Simulation (paper section 3.4)
Each worker node is assigned **fictional 2D coordinates** at startup. The master uses these coordinates when scheduling: it prefers to assign reduce tasks to workers whose coordinates are close to the map workers that hold the relevant intermediate files.

Inter-node RPC calls are **throttled** based on Euclidean distance between the two workers' fictional coordinates — simulating network latency in a real cluster. Close workers communicate fast; distant workers communicate slowly.

### Fault Simulation (paper section 3.3)
- **Worker failure**: kill the OS process. Master receives `{:nodedown, node}`, re-schedules all in-progress and completed-but-unfetched map tasks.
- **RPC throttling**: wraps cross-node calls with a configurable artificial delay, either distance-based or manually set per worker.

### Observability
- **Structured logs:** the master and workers emit structured log lines on every significant event — task assignment, completion, node death, re-scheduling, stale-completion drops, locality decisions.
- **Phoenix LiveView dashboard (`mr_dashboard`):** a 2D worker map with per-worker task state and live job progress, served at `http://localhost:4000` (started automatically by `mix mr.start`).

---

## Tech Stack

| Concern | Choice | Reason |
|---|---|---|
| Language | Elixir | Native distributed computing via OTP; GenServer, supervision trees, Erlang distribution built in |
| Worker isolation | Distributed Erlang nodes | Separate OS processes; real fault detection via `Node.monitor_nodes`; scales to multi-machine with no code changes |
| RPC | Erlang distribution (`GenServer.call` across nodes) | Native, zero-boilerplate, works transparently across machines |
| Intermediate storage | Local disk per worker + RPC fetch | Paper-faithful; fault semantics are correct (dead worker = inaccessible data) |
| Build tool | Mix (umbrella project) | Standard Elixir; separate apps for master, worker, and shared protocol |
| Observability (phase 1) | Structured logs via Logger | Simple, no dependencies, event stream reusable for dashboard |
| Observability (phase 2) | Phoenix LiveView | Idiomatic Elixir; real-time push over WebSocket |

---

## Project Structure

```
babys-first-mapreduce/
├── apps/
│   ├── mr_master/        # Master node — scheduling, fault detection, locality, output collection
│   ├── mr_worker/        # Worker node — map/reduce execution, file I/O, RPC, FileServer
│   ├── mr_protocol/      # Shared message types, the Task behaviour, distance/coords helpers
│   └── mr_dashboard/     # Phoenix LiveView dashboard
├── config/               # config.exs / runtime.exs + config docs (see config/README.md)
├── docs/                 # design spec, plans, MULTI_MACHINE_SETUP.md
├── scripts/              # generate_data.py (seeded sample-data generator)
├── sample-data/          # 20 generated text files (gitignored)
├── tmp/                  # Intermediate files written by map workers (gitignored)
├── output/               # Final reduce output collected on the master (gitignored)
└── CLAUDE.md             # This file
```

---

## Key Design Principles

- **Follow the paper first.** When a design decision is ambiguous, ask: what does section X of the paper say? Implement that.
- **Learning over cleverness.** Prefer explicit, readable code over terse Elixir idioms until the concepts are solid.
- **Phase discipline (historical).** The project was built in phases — (1) single-machine core + logs, (2) dashboard, (3) multi-machine — all now complete. New work should still keep unrelated concerns out of a single change.
- **Fault tolerance is not optional.** The master must handle `{:nodedown, node}` correctly from day one. It is the core of the paper's contribution.

---

## Sample Data

`sample-data/document_01.txt` through `document_20.txt`. Generated by `scripts/generate_data.py` (seeded, so every run produces an identical dataset). Each file has 700k–1.2M lines, 10 random words per line, drawn from a ~500-word vocabulary. Total ~1 GB.
