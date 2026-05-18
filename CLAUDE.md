# Baby's First MapReduce

## Project Purpose

This project has two objectives, in priority order:

1. **Learning** — Hands-on experience designing a distributed system from first principles, using Elixir and the Erlang/OTP distributed computing model. Key learning targets: Elixir/OTP, distributed Erlang nodes, fault tolerance via supervision, the MapReduce execution model, and the shuffle/RPC pattern.
2. **Portfolio** — A talking piece for moving from application development into systems/distributed systems engineering. The implementation should be paper-faithful enough to discuss in a technical interview.

The reference paper is `Mapreduce.pdf` (Dean & Ghemawat, Google, OSDI 2004). All design decisions trace back to it. When in doubt, follow the paper.

---

## What We Are Building

A small but faithful implementation of the MapReduce programming model, running on a single machine as a cluster of distributed Erlang nodes. The initial task is **word frequency count** across 20 large text files (`sample-data/`).

Later iterations will:
- Span 3 physical MacBooks (zero code changes — just connect Erlang nodes over LAN)
- Add a Phoenix LiveView dashboard for real-time visualisation

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
2. Map worker reads its chunk, emits `{word, 1}` pairs, buckets them into R partitions using `hash(word) mod R`, and writes each partition to its **local disk** (`tmp/<worker-id>/bucket-<r>.bin`).
3. Map worker notifies master with file locations on completion.
4. Master assigns reduce tasks to idle reduce workers, passing them the locations of all bucket-R files across all map workers.
5. Reduce worker fetches each bucket file via RPC from the relevant map worker node, sorts all pairs by key, and runs the reduce function (sum counts per word).
6. Reduce worker writes final output to `output/`.
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
**Phase 1** (current): Structured log lines emitted by master and workers on every significant event — task assignment, completion, node death, re-scheduling, locality decisions.

**Phase 2** (future): Phoenix LiveView dashboard showing a 2D worker map, task state per worker, job progress, and interactive fault injection controls.

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

## Project Structure (planned)

```
babys-first-mapreduce/
├── apps/
│   ├── mr_master/        # Master node — task scheduling, fault detection, locality
│   ├── mr_worker/        # Worker node — map and reduce execution, file I/O, RPC handlers
│   └── mr_protocol/      # Shared message types, serialisation, constants
├── sample-data/          # 20 generated text files (~1M lines each, word frequency input)
├── tmp/                  # Intermediate files written by map workers (gitignored)
├── output/               # Final reduce output (gitignored)
├── Mapreduce.pdf         # Reference paper (Dean & Ghemawat, OSDI 2004)
└── CLAUDE.md             # This file
```

---

## Key Design Principles

- **Follow the paper first.** When a design decision is ambiguous, ask: what does section X of the paper say? Implement that.
- **Learning over cleverness.** Prefer explicit, readable code over terse Elixir idioms until the concepts are solid.
- **Phase discipline.** Phase 1 = single machine, logs, core MapReduce working correctly. Phase 2 = dashboard. Phase 3 = multi-machine. Don't mix phases.
- **Fault tolerance is not optional.** The master must handle `{:nodedown, node}` correctly from day one. It is the core of the paper's contribution.

---

## Sample Data

`sample-data/document_01.txt` through `document_20.txt`. Generated by `generate_data.py`. Each file has 700k–1.2M lines, 10 random words per line, drawn from a ~500-word vocabulary. Total ~1 GB.
