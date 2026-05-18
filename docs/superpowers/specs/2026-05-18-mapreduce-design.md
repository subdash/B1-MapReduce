# MapReduce Design Spec
**Date:** 2026-05-18
**Reference:** `Mapreduce.pdf` (Dean & Ghemawat, Google, OSDI 2004)

---

## 1. Goals

Build a faithful implementation of the MapReduce programming model as described in the Google paper. The system must:

- Run a full MapReduce job across a cluster of distributed Erlang nodes on a single machine
- Implement a pluggable `Task` behaviour so any map/reduce computation can be expressed without modifying the framework
- Simulate locality-aware scheduling using fictional worker coordinates
- Simulate and recover from worker failures
- Support speculative backup task execution for stragglers
- Emit structured logs that make every scheduling and fault-recovery decision visible

Non-goals for phase 1: multi-machine deployment, Phoenix LiveView dashboard, authentication between nodes.

---

## 2. Programming Model

Users implement the `MrProtocol.Task` behaviour:

```elixir
defmodule MrProtocol.Task do
  @callback map(key :: String.t(), value :: String.t()) :: [{String.t(), term()}]
  @callback reduce(key :: String.t(), values :: [term()]) :: {String.t(), term()}
  @callback combine(key :: String.t(), values :: [term()]) :: {String.t(), term()}

  @optional_callbacks combine: 2
end
```

- `map/2` — receives a filename (key) and a single line of text (value); returns a list of `{intermediate_key, intermediate_value}` pairs.
- `reduce/2` — receives a key and all values for that key across all map outputs; returns a single `{key, result}` pair.
- `combine/2` — optional local pre-aggregation run on the map worker before writing to disk. Defaults to `reduce/2` if not overridden. Only valid for commutative and associative reduce functions.

### Word count reference implementation

```elixir
defmodule MrWorker.Tasks.WordCount do
  @behaviour MrProtocol.Task

  @impl true
  def map(_filename, line) do
    line |> String.split() |> Enum.map(fn word -> {word, 1} end)
  end

  @impl true
  def reduce(word, counts), do: {word, Enum.sum(counts)}

  @impl true
  def combine(word, counts), do: {word, Enum.sum(counts)}
end
```

---

## 3. Architecture

### 3.1 Umbrella structure

```
babys-first-mapreduce/
├── apps/
│   ├── mr_protocol/        # shared types, behaviours, constants
│   ├── mr_master/          # master node — scheduling, fault detection
│   └── mr_worker/          # worker node — map, reduce, file serving
├── sample-data/            # 20 input files (~1M lines each)
├── tmp/                    # intermediate files written by map workers (gitignored)
├── output/                 # final reduce output (gitignored)
├── Mapreduce.pdf
└── CLAUDE.md
```

### 3.2 Distributed Erlang nodes

Each component runs as a separate Erlang node (OS process). At runtime:

```
master@localhost     — one per job
worker1@localhost    — N workers, all running mr_worker
worker2@localhost
...
workerN@localhost
```

Workers connect to the master by calling `Node.connect(:master@localhost)` at startup. The master calls `Node.monitor_nodes(true)` so that worker death produces an automatic `{:nodedown, node}` message in the master's mailbox.

All inter-node calls use Erlang distribution: `GenServer.call({pid_or_name, node}, message)`. No HTTP, no gRPC.

### 3.3 Startup

A single Mix task starts the full cluster:

```
mix mr.start --workers 6 --task WordCount --input sample-data/
```

This task:
1. Starts the master node in the current process
2. Spawns N worker OS processes via `System.cmd/3`, each with a unique node name and randomly assigned fictional coordinates
3. Waits for all workers to register with the master
4. Submits the job

---

## 4. Master

### 4.1 State

The master GenServer holds:

```elixir
%Master.State{
  workers: %{node => %WorkerInfo{node, coords, status}},
  map_tasks: %{task_id => %MapTask{id, file_path, byte_range, num_reducers, status, assigned_to}},
  reduce_tasks: %{task_id => %ReduceTask{id, bucket, locations, status, assigned_to}},
  intermediate_locations: %{task_id => [{node, file_path}]},
  task_module: module(),
  num_reducers: pos_integer()
}
```

Task status is one of: `:idle | :in_progress | :completed`.

### 4.2 Job lifecycle

1. Receive `{:submit_job, opts}` — list input files, create M map tasks (one per file), set status to `:idle`.
2. Assign idle map tasks to idle workers (round-robin for now — map tasks have no data locality in phase 1 since all input files are on the shared filesystem).
3. As map tasks complete, store intermediate file locations reported by the worker.
4. When all map tasks are `:completed`, create R reduce tasks, each carrying the full list of `{node, file_path}` pairs for bucket R across all map workers.
5. Assign idle reduce tasks to idle workers (locality-aware).
6. When all reduce tasks are `:completed`, log job summary and terminate.

### 4.3 Worker registration

Workers call `GenServer.call(Master, {:register, %WorkerInfo{}})` on boot. The master adds them to its registry with status `:idle`.

---

## 5. Worker

### 5.1 Map task execution

On receiving `{:run_map, %MapTask{}, task_module}`:

1. Open the assigned file (or byte range within it).
2. For each line, call `task_module.map(filename, line)` → list of `{key, value}` pairs.
3. Bucket each pair into one of R partitions: `bucket = :erlang.phash2(key, num_reducers)`.
4. If `task_module` exports `combine/2`, apply it to collapse duplicate keys within each bucket before writing.
5. Encode each bucket with `:erlang.term_to_binary/1` and write to `tmp/<node_name>/map-<task_id>-bucket-<r>.bin`.
6. Reply `{:map_done, task_id, bucket_locations}` where `bucket_locations` is `%{bucket_index => file_path}`.

### 5.2 Reduce task execution

On receiving `{:run_reduce, %ReduceTask{}, task_module}`:

1. For each `{node, file_path}` in the task's location list, call `MrWorker.RPC.call(node, FileServer, {:fetch, file_path})` to retrieve the encoded bucket.
2. Decode with `:erlang.binary_to_term/1` and merge all fetched pairs into one list.
3. Sort by key.
4. Group by key and call `task_module.reduce(key, values)` for each unique key.
5. Write results to `output/bucket-<r>.txt` as `"word\tcount\n"` lines.
6. Reply `{:reduce_done, task_id}`.

### 5.3 File server

Each worker runs a `FileServer` GenServer that handles `{:fetch, file_path}` calls from reduce workers. It reads the file and returns the binary. This is the RPC fetch step from paper section 3.1.

---

## 6. Locality-Aware Scheduling

Each `%WorkerInfo{}` carries `coords: {x, y}` — a random point in a 100×100 fictional grid assigned at registration.

### Assignment algorithm

When assigning a reduce task:

1. Collect the set of map worker nodes that hold intermediate files for this bucket.
2. For each idle worker, compute the mean Euclidean distance to those map worker nodes.
3. Assign to the idle worker with the lowest mean distance.

For map tasks (where any worker is equally valid), assign to the idle worker with the most available capacity (round-robin initially, locality-aware in phase 2 once we have data locality on splits).

### RPC latency throttling

All cross-node calls go through `MrWorker.RPC.call/3` instead of `GenServer.call/3` directly:

```elixir
def call(target_node, server, message) do
  delay = compute_delay(self_coords(), worker_coords(target_node))
  Process.sleep(delay)
  GenServer.call({server, target_node}, message)
end
```

`compute_delay/2` returns `round(distance * @latency_factor_ms)`. `@latency_factor_ms` is a configurable constant (default: 2ms per unit distance). Maximum simulated delay is capped at 500ms.

Each worker maintains a local copy of the full worker coordinate registry. The master sends the current registry to a worker when it assigns a task (`{:run_map, task, task_module, worker_registry}`), so every worker always has up-to-date coords for all peers it may need to contact.

### Manual throttling

The master can send `{:set_throttle, multiplier}` to any worker, overriding its `latency_factor` for subsequent calls. Used to simulate degraded nodes.

---

## 7. Fault Tolerance

### 7.1 Worker death (paper section 3.3)

The master handles `{:nodedown, node}` by:

1. Marking the worker as `:dead` in the registry.
2. Re-queuing all `:in_progress` tasks assigned to that node back to `:idle`.
3. Re-queuing all `:completed` **map** tasks assigned to that node back to `:idle` — because their intermediate files are now unreachable.
4. Completed **reduce** tasks are not re-queued — their output is in `output/` on the shared filesystem.
5. Resuming normal assignment from the updated idle task queue.

### 7.2 Backup tasks / straggler mitigation (paper section 3.6)

A periodic `:check_stragglers` message fires in the master every 10 seconds. When the job is past 80% completion:

1. Find all tasks still `:in_progress`.
2. For each, if a second idle worker is available and no backup is already running, assign a backup execution of the same task.
3. Whichever execution completes first — primary or backup — marks the task `:completed`. The other's result is discarded.

---

## 8. Message Protocol

All message shapes are defined in `mr_protocol` as tagged tuples. Key messages:

| Direction | Message | Meaning |
|---|---|---|
| Worker → Master | `{:register, %WorkerInfo{}}` | Worker online, here are my coords |
| Master → Worker | `{:run_map, %MapTask{}, module, registry}` | Execute this map task; registry is current worker coord map |
| Master → Worker | `{:run_reduce, %ReduceTask{}, module, registry}` | Execute this reduce task; registry is current worker coord map |
| Worker → Master | `{:map_done, task_id, locations}` | Map complete, here are file locations |
| Worker → Master | `{:reduce_done, task_id}` | Reduce complete |
| Master → Worker | `{:set_throttle, multiplier}` | Override latency factor |
| Reducer → Worker | `{:fetch, file_path}` | Fetch intermediate file (via FileServer) |
| Erlang runtime → Master | `{:nodedown, node}` | Worker node died |

---

## 9. Structured Logging

Every significant event emits a `Logger.info/1` line in the format:
`[role] event | key=value ...`

Key events:

```
[master] job_started      | task=WordCount files=20 reducers=4 workers=6
[master] task_assigned    | type=map id=3 worker=worker2@localhost dist=14.2
[master] task_assigned    | type=reduce id=1 worker=worker1@localhost dist=8.7
[master] node_down        | node=worker4@localhost requeued=2
[master] task_assigned    | type=map id=3 worker=worker1@localhost reason=requeue
[master] backup_launched  | task=map:7 primary=worker3 backup=worker5
[master] job_complete     | duration_ms=42300 map_tasks=20 reduce_tasks=4
[worker] map_done         | id=3 pairs_emitted=94201 duration_ms=1820
[worker] reduce_done      | id=1 keys=412 duration_ms=3100
```

---

## 10. File Layout at Runtime

```
tmp/
  worker1@localhost/
    map-3-bucket-0.bin
    map-3-bucket-1.bin
    ...
  worker2@localhost/
    map-7-bucket-0.bin
    ...

output/
  bucket-0.txt    # "the\t94231\nand\t87442\n..."
  bucket-1.txt
  ...
```

---

## 11. Phase 2 (Future)

- **Multi-machine:** Connect worker nodes on separate MacBooks. Zero code changes — Erlang distribution handles it. Requires shared cookie and network-accessible node names.
- **LiveView dashboard:** Phoenix app showing 2D worker map, task states, job progress bar, and interactive fault injection (kill/throttle buttons per worker).
- **Additional tasks:** Distributed grep, inverted index (both described in paper section 2.3) as further `Task` behaviour implementations.
