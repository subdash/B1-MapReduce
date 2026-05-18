# MapReduce Implementation Plan

> **Workflow:** You write the code. Consult Claude for advice and submit each task for review before moving on. The review checkpoint at the end of each task is the handoff point.

**Goal:** Build a distributed MapReduce framework in Elixir with pluggable task behaviour, locality-aware scheduling, fault tolerance, and simulated network latency.

**Architecture:** Three-app Elixir umbrella. Workers run as separate Erlang nodes (OS processes) connected to a master node over Erlang distribution. Intermediate data lives on each worker's local disk; reduce workers fetch it via RPC.

**Tech Stack:** Elixir, OTP (GenServer, Application, Supervisor), Erlang distribution, ExUnit

**Spec:** `docs/superpowers/specs/2026-05-18-mapreduce-design.md`

---

## File Structure

```
apps/
  mr_protocol/
    lib/mr_protocol/
      task.ex             # Task behaviour (map/reduce/combine callbacks)
      worker_info.ex      # %WorkerInfo{} struct
      map_task.ex         # %MapTask{} struct
      reduce_task.ex      # %ReduceTask{} struct
    test/mr_protocol/
      task_test.exs

  mr_master/
    lib/mr_master/
      application.ex      # OTP Application + supervision tree
      master.ex           # Master GenServer — all job state lives here
      job.ex              # Input splitting: file list → [%MapTask{}]
      scheduler.ex        # Locality-aware worker selection
    test/mr_master/
      job_test.exs
      scheduler_test.exs
      master_test.exs

  mr_worker/
    lib/mr_worker/
      application.ex      # OTP Application + supervision tree
      worker.ex           # Worker GenServer — accepts task assignments
      map_task.ex         # Map phase execution logic
      reduce_task.ex      # Reduce phase execution logic
      file_server.ex      # GenServer: serves intermediate files on request
      rpc.ex              # Wraps GenServer.call with distance-based latency
      tasks/
        word_count.ex     # Reference Task implementation
    test/mr_worker/
      map_task_test.exs
      reduce_task_test.exs
      file_server_test.exs
      rpc_test.exs
      tasks/
        word_count_test.exs

lib/
  mix/tasks/
    mr.start.ex           # `mix mr.start` — boots the full cluster locally
```

---

## Elixir Concepts to Know Before Starting

Before writing any code, read or skim these concepts in the official Elixir docs (https://elixir-lang.org/getting-started/):

- **GenServer** — the building block for stateful processes. You'll use `GenServer.start_link`, `handle_call`, `handle_cast`, `handle_info`.
- **Supervisor / Application** — how OTP starts and restarts processes. Each umbrella app has an `Application` that starts a `Supervisor`.
- **Behaviours** — Elixir's version of interfaces. `@callback` declares a required function; `@optional_callbacks` marks optional ones; `@behaviour ModuleName` in a module declares it implements the behaviour.
- **Structs** — `defstruct` creates a named map with defined fields and defaults.
- **Pattern matching in function heads** — used heavily in GenServer callbacks.
- **ExUnit** — the built-in test framework. `use ExUnit.Case` in test files. `mix test` runs all tests.
- **Node** — in distributed Erlang, a node is a running Erlang VM. `node()` returns your current node name. `Node.connect/1` connects to another node. `Node.monitor_nodes(true)` sends `{:nodedown, node}` to your process when any node goes down.

---

## Task 1: Bootstrap the Umbrella Project

**What to build:** The Elixir project skeleton — root umbrella `mix.exs` and three sub-apps.

**Why it matters:** An umbrella lets each app be compiled and tested independently while sharing a single repo and build system.

**Steps:**

1. Create the root umbrella `mix.exs` by hand (the directory already exists with other files, so you can't use `mix new` directly). It should set `apps_path: "apps"` and list no top-level deps. The module name should be `BabysFirstMapreduce.MixProject`.

2. Create `apps/` and generate each sub-app from the repo root:
   ```bash
   mix new apps/mr_protocol
   mix new apps/mr_master --sup
   mix new apps/mr_worker --sup
   ```
   The `--sup` flag generates an `Application` module and supervision tree, which `mr_master` and `mr_worker` need. `mr_protocol` is a library with no runtime processes, so it doesn't need `--sup`.

3. Add inter-app dependencies. In `apps/mr_master/mix.exs` and `apps/mr_worker/mix.exs`, add to `deps`:
   ```elixir
   {:mr_protocol, in_umbrella: true}
   ```

4. Create a root `config/config.exs` (umbrella projects expect this file):
   ```elixir
   import Config
   ```

5. Verify the setup compiles:
   ```bash
   mix deps.get
   mix compile
   mix test   # should show 0 failures (just the default hello-world tests)
   ```

> **Review checkpoint:** Share your `mix.exs` files (root + all three apps) for review before moving on.

---

## Task 2: `MrProtocol.Task` Behaviour

**Files:** `apps/mr_protocol/lib/mr_protocol/task.ex`, `apps/mr_protocol/test/mr_protocol/task_test.exs`

**What to build:** The behaviour that defines the MapReduce programming model. This is the user-facing API — any computation (word count, distributed grep, inverted index) is expressed by implementing this behaviour.

**Key concept:** A behaviour in Elixir is a module with `@callback` declarations. Another module opts in with `@behaviour ModuleName` and implements each required callback. `@optional_callbacks` marks callbacks that don't have to be implemented.

**What the behaviour declares:**
- `map/2` — takes `(key :: String.t(), value :: String.t())`, returns `[{String.t(), term()}]`. For word count, key is the filename and value is one line; it returns a list of `{"word", 1}` tuples.
- `reduce/2` — takes `(key :: String.t(), values :: [term()])`, returns `{String.t(), term()}`. For word count, it sums the list of 1s.
- `combine/2` — same signature as `reduce/2`, but optional. It's a local pre-aggregation step that runs on the map worker before writing to disk (see spec section 2 and paper section 4.3).

**Test to write first:** Verify that a module implementing the behaviour compiles without warnings, and that calling `map`, `reduce`, and `combine` on it returns the expected shapes. Use a minimal inline test implementation (a module defined inside the test file using `defmodule`).

> **Review checkpoint:** Share `task.ex` and `task_test.exs`.

---

## Task 3: Protocol Data Structs

**Files:** `apps/mr_protocol/lib/mr_protocol/worker_info.ex`, `map_task.ex`, `reduce_task.ex`

**What to build:** Three structs that are passed in messages between master and workers. Defining them in `mr_protocol` means both `mr_master` and `mr_worker` can use them without circular dependencies.

**`%MrProtocol.WorkerInfo{}`**
Fields:
- `node` — the Erlang node name (an atom, e.g. `:"worker1@localhost"`)
- `coords` — fictional position as `{float, float}`, a point in a 100×100 grid
- `status` — one of `:idle | :busy | :dead`, default `:idle`

**`%MrProtocol.MapTask{}`**
Fields:
- `id` — unique integer
- `file_path` — full path to the input file
- `num_reducers` — how many reduce buckets to split output into
- `status` — `:idle | :in_progress | :completed`, default `:idle`
- `assigned_to` — node atom or `nil`

**`%MrProtocol.ReduceTask{}`**
Fields:
- `id` — unique integer
- `bucket` — which reduce bucket index (0 to num_reducers-1) this task handles
- `locations` — list of `{node, file_path}` tuples — where to fetch intermediate data from
- `status` — `:idle | :in_progress | :completed`, default `:idle`
- `assigned_to` — node atom or `nil`

**Tests to write:** Basic struct construction — verify defaults, field names, and that you can update fields using the struct update syntax (`%task | status: :completed`).

> **Review checkpoint:** Share all three struct files and tests.

---

## Task 4: `MrMaster.Job` — Input Splitting

**Files:** `apps/mr_master/lib/mr_master/job.ex`, `apps/mr_master/test/mr_master/job_test.exs`

**What to build:** A pure function module (no GenServer, no state) that turns a directory of input files into a list of `%MapTask{}` structs, one per file.

**Key function:** `create_map_tasks(input_dir, num_reducers)` — lists files in `input_dir`, sorts them, and returns a list of `%MapTask{}` structs with sequential IDs starting at 1. The `file_path` on each task should be the full absolute path.

**Tests to write:** Create a temporary directory with a few empty files, call `create_map_tasks/2`, and assert the returned list has the right length, sequential IDs, correct file paths, and all tasks start with status `:idle`.

**Elixir hint:** `File.ls!/1` lists a directory. `Path.join/2` builds a full path. `Enum.with_index/2` adds an index to each element.

> **Review checkpoint:** Share `job.ex` and `job_test.exs`.

---

## Task 5: `MrMaster.Scheduler` — Locality-Aware Assignment

**Files:** `apps/mr_master/lib/mr_master/scheduler.ex`, `apps/mr_master/test/mr_master/scheduler_test.exs`

**What to build:** A pure function module implementing the locality-aware worker selection algorithm from spec section 6. No GenServer — just functions that take the worker registry map and return a node to assign to.

**Functions to implement:**

`distance(coords1, coords2)` — Euclidean distance between two `{x, y}` points. Use `:math.sqrt` and `:math.pow`.

`assign_map_task(workers)` — Given the worker registry (`%{node => %WorkerInfo{}}`), return the node name of the first idle worker (round-robin for now — map tasks have no locality advantage since all input files are on the shared filesystem). Return `nil` if no idle workers.

`assign_reduce_task(workers, map_worker_nodes)` — Given the worker registry and a list of map worker nodes that hold the relevant intermediate files, return the idle worker whose mean Euclidean distance to those map workers is smallest. Return `nil` if no idle workers.

**Tests to write:** This is a pure function module — all tests are straightforward. Test `distance/2` with known values (e.g., a 3-4-5 right triangle). Test `assign_map_task/1` with a mix of idle and busy workers. Test `assign_reduce_task/2` with several workers at known positions and verify it picks the closest one.

> **Review checkpoint:** Share `scheduler.ex` and `scheduler_test.exs`. The scheduler is a good interview talking point — be ready to explain the locality algorithm.

---

## Task 6: `MrMaster.Master` — Worker Registration and Job Submission

**Files:** `apps/mr_master/lib/mr_master/master.ex`, `apps/mr_master/lib/mr_master/application.ex`, `apps/mr_master/test/mr_master/master_test.exs`

**What to build:** The Master GenServer — the central coordinator that holds all job state. This task covers the first part: starting up, accepting worker registrations, and receiving a job submission.

**Key concept:** A GenServer has an `init/1` callback that returns the initial state, then `handle_call/3` for synchronous messages and `handle_cast/2` or `handle_info/2` for async ones. State is passed through every callback as the last argument and returned as the last element of the reply tuple.

**State shape** (a plain map or a struct — your choice):
```
%{
  workers: %{},                    # node => %WorkerInfo{}
  map_tasks: %{},                  # id => %MapTask{}
  reduce_tasks: %{},               # id => %ReduceTask{}
  intermediate_locations: %{},     # task_id => %{bucket => file_path}
  task_module: nil,
  num_reducers: 0,
  phase: :waiting                  # :waiting | :mapping | :reducing | :done
}
```

**Messages to handle in this task:**
- `{:register, %WorkerInfo{}}` (call) — add the worker to the registry with status `:idle`. Reply `:ok`. Also call `Node.monitor_nodes(true)` in `init/1` so node deaths are automatically delivered.
- `{:submit_job, opts}` (call) — `opts` is a keyword list with `:input_dir`, `:task_module`, `:num_reducers`. Call `Job.create_map_tasks/2`, store them in state, set phase to `:mapping`, and assign available tasks to idle workers. Reply `:ok`.

**The assignment loop:** Write a private function (e.g., `assign_pending_tasks/1`) that takes the full state, looks for idle tasks + idle workers, uses the Scheduler to pick workers, and returns updated state. Call it any time the task queue or worker availability changes.

**`application.ex`:** Register the Master under its module name so workers can call it by name without knowing its PID:
```elixir
children = [{MrMaster.Master, name: MrMaster.Master}]
```

**Tests:** Start the master in each test using `start_supervised/1`. Test that a registered worker appears in the worker list. Test that submitting a job creates map tasks and assigns them to idle workers.

> **Review checkpoint:** Share `master.ex`, `application.ex`, and the test file through the registration + job submission message handlers.

---

## Task 7: `MrMaster.Master` — Map Phase Completion

**Files:** `apps/mr_master/lib/mr_master/master.ex` (continuing), `apps/mr_master/test/mr_master/master_test.exs` (continuing)

**What to build:** The master's response when a worker reports a completed map task, and the transition to the reduce phase when all map tasks are done.

**Message to handle:**
- `{:map_done, task_id, bucket_locations}` (cast) — `bucket_locations` is `%{bucket_index => file_path}`. Mark the task `:completed`, store the locations in `intermediate_locations`, mark the worker `:idle`, call `assign_pending_tasks/1`. If all map tasks are now `:completed`, transition to the reduce phase.

**Reduce phase transition:** Write a private function `maybe_start_reduce/1` that checks if all map tasks are `:completed`. If so, create reduce tasks using the accumulated `intermediate_locations`. Each reduce task for bucket R needs all `{node, file_path}` pairs where `file_path` is the bucket-R file across every map worker. Set phase to `:reducing` and call `assign_pending_tasks/1`.

**Tests:** In a test, register a worker, submit a job with a single-file input directory, simulate the map completion by casting `{:map_done, ...}` to the master, and verify reduce tasks are created with the correct locations.

> **Review checkpoint:** Share the updated `master.ex` through the map completion and reduce phase transition logic.

---

## Task 8: `MrMaster.Master` — Reduce Phase and Job Completion

**Files:** `apps/mr_master/lib/mr_master/master.ex` (continuing)

**What to build:** Handling reduce task completion and declaring the job done.

**Message to handle:**
- `{:reduce_done, task_id}` (cast) — mark the task `:completed`, mark the worker `:idle`. If all reduce tasks are now `:completed`, set phase to `:done` and emit a job summary log line: `[master] job_complete | duration_ms=X map_tasks=N reduce_tasks=R`.

**Logging:** Use `require Logger` and `Logger.info/1`. Follow the log format from spec section 9. Log on: job started, every task assignment (including worker node and distance), job complete.

**Tests:** Simulate a full job lifecycle in a test — register worker, submit job, send map_done, send reduce_done, verify phase becomes `:done`.

> **Review checkpoint:** Share the completed job lifecycle through reduce phase.

---

## Task 9: `MrMaster.Master` — Fault Tolerance

**Files:** `apps/mr_master/lib/mr_master/master.ex` (continuing), `apps/mr_master/test/mr_master/master_test.exs` (continuing)

**What to build:** The master's response to a worker dying. This is the core of the paper's fault tolerance contribution (section 3.3).

**Message to handle:**
- `{:nodedown, node}` (info) — Erlang delivers this automatically because you called `Node.monitor_nodes(true)` in `init/1`. Handle it in `handle_info/3`. Mark the worker `:dead`. Re-queue:
  - All `:in_progress` map tasks assigned to that node → back to `:idle`, `assigned_to: nil`
  - All `:completed` map tasks assigned to that node → back to `:idle`, `assigned_to: nil` (intermediate files are gone)
  - `:in_progress` reduce tasks assigned to that node → back to `:idle`
  - `:completed` reduce tasks → leave them (output is in `output/` on the shared filesystem)
  - Call `assign_pending_tasks/1` with the updated state.
  - Log: `[master] node_down | node=X requeued=N`

**Also handle `:set_throttle`:**
- `{:set_throttle, node, multiplier}` (cast) — update the target worker's throttle_multiplier in the registry. The worker's own registry copy gets updated when the next task is assigned (the registry is passed with each task).

**Tests:** In a test, simulate a nodedown by sending `{:nodedown, some_node}` directly to the master process. Verify that tasks are re-queued correctly. You can use `Process.send/3` to inject info messages in tests.

> **Review checkpoint:** Share the fault tolerance implementation and tests. This is one of the most important pieces to get right.

---

## Task 10: `MrMaster.Master` — Backup Tasks (Straggler Mitigation)

**Files:** `apps/mr_master/lib/mr_master/master.ex` (continuing)

**What to build:** The speculative re-execution of slow tasks described in paper section 3.6.

**How it works:** In `init/1`, schedule a recurring timer: `Process.send_after(self(), :check_stragglers, 10_000)`. In `handle_info(:check_stragglers, state)`, reschedule the timer and then: if the job is past 80% completion (count completed tasks / total tasks > 0.8), find any tasks still `:in_progress` and, if a second idle worker is available, assign a backup execution of the same task to it. Track backup executions so you don't double-launch them. When either the primary or the backup sends a completion message, accept it and ignore any subsequent completion for the same task ID.

**State addition:** Add `backup_tasks: %{}` to your state — a map from `task_id` to `backup_worker_node`. Check this before launching a backup to avoid duplicates. When a task completes, delete its entry from `backup_tasks`.

**Log:** `[master] backup_launched | task=map:7 primary=worker3 backup=worker5`

**Tests:** This one is harder to test in isolation — write a test that sets up a near-complete job with one straggling task and two idle workers, injects `:check_stragglers`, and verifies the task gets assigned to a second worker.

> **Review checkpoint:** Share the straggler mechanism. Consider whether your `assign_pending_tasks/1` needs changes to support backup assignments.

---

## Task 11: `WordCount` Task

**Files:** `apps/mr_worker/lib/mr_worker/tasks/word_count.ex`, `apps/mr_worker/test/mr_worker/tasks/word_count_test.exs`

**What to build:** The reference implementation of `MrProtocol.Task`. This is the computation — the framework does everything else.

**`map/2`:** Receives `(filename, line)`. Splits the line on whitespace and returns a list of `{"word", 1}` tuples. Each word is one entry. Ignore empty strings from splitting.

**`reduce/2`:** Receives `(word, counts)` where counts is a list of integers. Returns `{word, Enum.sum(counts)}`.

**`combine/2`:** Same as `reduce/2` — for an associative+commutative function like sum, the combiner is identical to the reducer.

**Tests:** Pure unit tests. Test `map/2` with a line containing multiple words and verify the output pairs. Test `reduce/2` with a list of counts. Test `combine/2` the same way. Test `map/2` with an empty line.

> **Review checkpoint:** Share `word_count.ex` and tests. This module should be simple — if it's getting complicated, something is wrong.

---

## Task 12: `MrWorker.MapTask` — Map Phase Execution

**Files:** `apps/mr_worker/lib/mr_worker/map_task.ex`, `apps/mr_worker/test/mr_worker/map_task_test.exs`

**What to build:** The logic that executes a single map task. This is a pure function module (no GenServer) — it reads a file, runs the user's map function, buckets the output, optionally combines, and writes intermediate files to disk.

**Main function:** `execute(task, task_module, worker_node_name)` — returns `{:ok, bucket_locations}` where `bucket_locations` is `%{bucket_index => file_path}`.

**Steps inside `execute/3`:**
1. Read the file at `task.file_path` line by line using `File.stream!/1`.
2. For each line, call `task_module.map(task.file_path, line)` and collect all `{key, value}` pairs.
3. Bucket each pair: `bucket = :erlang.phash2(key, task.num_reducers)`.
4. Group pairs by bucket index.
5. If `task_module` exports `combine/2` (check with `function_exported?(task_module, :combine, 2)`), apply it per bucket: group by key, call `combine(key, values)` for each key, replace the raw pairs with the combined output.
6. For each bucket, encode its pairs with `:erlang.term_to_binary/1` and write to `tmp/<worker_node_name>/map-<task.id>-bucket-<bucket>.bin`. Create the directory with `File.mkdir_p!/1` if it doesn't exist.
7. Return `{:ok, %{bucket => file_path}}`.

**Tests:** Create a `%MapTask{}` pointing at a small temp file you write in the test. Call `execute/3` with `WordCount`. Verify the output files exist, decode them with `:erlang.binary_to_term/1`, and check the contents. Test that the combiner collapses duplicate keys within a bucket.

**Note on file paths:** Use `Path.join(["tmp", worker_node_name, "map-#{task.id}-bucket-#{bucket}.bin"])` but resolve it relative to the project root, not the test directory. In tests, use `System.tmp_dir!/0` for isolation.

> **Review checkpoint:** Share `map_task.ex` and tests. The combiner logic is subtle — make sure the test covers a case with repeated words.

---

## Task 13: `MrWorker.FileServer` and `MrWorker.RPC`

**Files:** `apps/mr_worker/lib/mr_worker/file_server.ex`, `apps/mr_worker/lib/mr_worker/rpc.ex`, `apps/mr_worker/test/mr_worker/file_server_test.exs`, `apps/mr_worker/test/mr_worker/rpc_test.exs`

**What to build:** Two small modules. `FileServer` serves intermediate files to reduce workers on request. `RPC` wraps `GenServer.call` with simulated network latency.

---

**`MrWorker.FileServer`**

A GenServer registered under its module name. It handles one message:
- `{:fetch, file_path}` (call) — reads the file at `file_path` and returns the binary. Return `{:error, :not_found}` if the file doesn't exist.

The `FileServer` must be started in `mr_worker`'s supervision tree (in `application.ex`).

**Tests:** Start the FileServer in a test, write a temp file, call `GenServer.call(MrWorker.FileServer, {:fetch, path})` and verify the binary matches.

---

**`MrWorker.RPC`**

A plain function module (not a GenServer). Its job is to wrap `GenServer.call` with a latency delay based on the fictional distance between the calling worker and the target worker.

**Main function:** `call(target_node, server, message, registry, throttle_multiplier \\ 1.0)`

- Look up the calling node's coords from `registry[node()]` and the target node's coords from `registry[target_node]`.
- Compute Euclidean distance. Note: `MrWorker` does not depend on `MrMaster`, so you cannot import from `MrMaster.Scheduler`. Either duplicate the distance calculation here (it's one line with `:math.sqrt` and `:math.pow`), or move it into `MrProtocol` as a shared utility function — whichever feels cleaner to you.
- Compute delay: `round(distance * 2.0 * throttle_multiplier)` milliseconds, capped at 500ms.
- `Process.sleep(delay)`, then `GenServer.call({server, target_node}, message)`.
- If either node is missing from the registry, skip the delay and call directly.

**Tests:** Test `call/5` with a registry containing two workers at known positions. Verify the delay is approximately correct (use `:timer.tc/1` to measure elapsed time). Since actual sleeping makes tests slow, keep test distances small (e.g., distance of 1 unit = 2ms delay).

> **Review checkpoint:** Share both modules and tests.

---

## Task 14: `MrWorker.ReduceTask` — Reduce Phase Execution

**Files:** `apps/mr_worker/lib/mr_worker/reduce_task.ex`, `apps/mr_worker/test/mr_worker/reduce_task_test.exs`

**What to build:** The logic that executes a single reduce task. Fetches intermediate files from map workers, sorts and groups by key, runs reduce, and writes final output.

**Main function:** `execute(task, task_module, registry, throttle_multiplier)` — returns `:ok`.

**Steps inside `execute/4`:**
1. For each `{node, file_path}` in `task.locations`, call `MrWorker.RPC.call(node, MrWorker.FileServer, {:fetch, file_path}, registry, throttle_multiplier)` to retrieve the binary.
2. Decode each binary with `:erlang.binary_to_term/1` to get a list of `{key, value}` pairs.
3. Concatenate all pairs from all locations into one flat list.
4. Sort the list by key: `Enum.sort_by(pairs, fn {k, _} -> k end)`.
5. Group by key: `Enum.group_by(pairs, fn {k, _} -> k end, fn {_, v} -> v end)` gives you `%{key => [values]}`.
6. For each key, call `task_module.reduce(key, values)` → `{key, result}`.
7. Write results to `output/bucket-#{task.bucket}.txt`, one line per key: `"#{key}\t#{result}\n"`. Create the directory with `File.mkdir_p!/1`.
8. Return `:ok`.

**Tests:** Write intermediate `.bin` files by hand in the test (use `:erlang.term_to_binary/1`). Create a `%ReduceTask{}` with locations pointing at those files. Since the files are local, use the current node in the location tuples and skip real RPC (call `FileServer` directly, or stub it by having the test write files to a path the FileServer can reach). Run `execute/4` and verify the output file contents.

> **Review checkpoint:** Share `reduce_task.ex` and tests.

---

## Task 15: `MrWorker.Worker` GenServer

**Files:** `apps/mr_worker/lib/mr_worker/worker.ex`, `apps/mr_worker/lib/mr_worker/application.ex`

**What to build:** The Worker GenServer — the entry point for a worker node. It connects to the master, registers itself, and handles incoming task assignments.

**`init/1`:**
- Receives `[master_node: atom, coords: {float, float}]` from the application.
- Connects to the master: `Node.connect(master_node)`.
- Registers with the master: `GenServer.call({MrMaster.Master, master_node}, {:register, %MrProtocol.WorkerInfo{node: node(), coords: coords}})`.
- Returns initial state: `%{coords: coords, master_node: master_node, throttle_multiplier: 1.0}`.

**Messages to handle:**
- `{:run_map, task, task_module, registry}` (cast) — spawn a `Task` (using `Task.start/1`) to run `MrWorker.MapTask.execute/3`. When the task finishes, the spawned process should cast `{:map_done, task.id, locations}` back to the master via `GenServer.cast({MrMaster.Master, state.master_node}, {:map_done, task.id, locations})`. Capture `state.master_node` before entering the spawned task — closures capture variables from the enclosing scope. The Worker GenServer itself should not block. Log: `[worker] map_started | id=X`.
- `{:run_reduce, task, task_module, registry}` (cast) — same pattern but for `MrWorker.ReduceTask.execute/4`. Cast `{:reduce_done, task.id}` when done. Log: `[worker] reduce_started | id=X`.
- `{:set_throttle, multiplier}` (cast) — update `throttle_multiplier` in state.

**Key concept — why spawn a Task:** If the Worker GenServer ran the map/reduce computation directly (blocking in `handle_cast`), it couldn't handle any other messages (like `:set_throttle`) while working. Spawning a separate `Task` process keeps the GenServer's mailbox responsive.

**`application.ex`:** Read `master_node` and `coords` from application config. Start `MrWorker.FileServer` and `MrWorker.Worker` in the supervision tree.

> **Review checkpoint:** Share `worker.ex` and `application.ex`. Pay close attention to how task results are reported back to the master — the spawned Task needs the master's node name from the Worker's state.

---

## Task 16: `mix mr.start` — Cluster Startup

**Files:** `lib/mix/tasks/mr.start.ex`

**What to build:** A Mix task that boots the full cluster locally — starts the master node and spawns N worker OS processes, then submits the job.

**Note on Mix tasks:** A Mix task is a module named `Mix.Tasks.SomeName` that implements `run/1`. The file lives in `lib/mix/tasks/` at the umbrella root (not inside any app). Run it with `mix mr.start`.

**CLI arguments to accept:**
- `--workers N` — number of worker processes to spawn (default: 4)
- `--task MODULE` — the Task module to use (default: `MrWorker.Tasks.WordCount`)
- `--input DIR` — input directory (default: `sample-data/`)
- `--reducers N` — number of reduce buckets (default: 4)

**What `run/1` does:**
1. Parse args with `OptionParser.parse/2`.
2. Start the master node. Since the Mix task is already running in an Erlang node, you need to ensure the node has a name. Set up the node name and cookie in `config/config.exs` for the master, or pass `--name master@localhost` in the startup command.
3. Spawn N worker processes. Use `Port.open/2` with `{:spawn_executable, System.find_executable("elixir")}` and args `["--name", "workerN@localhost", "--cookie", "secret", "-S", "mix", "run", "--no-halt"]`. Each worker should get unique `--eval` args or config to set its master_node and coords.
4. Wait for all N workers to register (poll the master state or add a `{:wait_for_workers, n}` call on the master).
5. Submit the job: `GenServer.call(MrMaster.Master, {:submit_job, input_dir: dir, task_module: module, num_reducers: n})`.
6. Block until the job is done (add a `{:wait_for_completion}` call to the master that blocks until phase == :done).
7. Print a summary.

**Alternatively (simpler first cut):** Print the commands for each worker and let the user open terminals manually. This is fine for initial testing.

> **Review checkpoint:** Share the Mix task. Discuss the startup sequencing — this is where subtle distributed bugs can appear (what if a worker tries to register before the master has started its GenServer?).

---

## Task 17: End-to-End Integration Test

**What to build:** A manual end-to-end test running the full job on a subset of `sample-data/`.

**Steps:**
1. Run `mix mr.start --workers 4 --reducers 4 --input sample-data/` (or start workers manually in separate terminals).
2. Watch the logs and verify you can see:
   - Workers registering
   - Map tasks being assigned (with distances logged)
   - Map task completions
   - Reduce tasks being assigned (with locality decisions visible)
   - Reduce task completions
   - Job complete log line
3. Check `output/bucket-*.txt` files. Spot-check a few words by running a simple count on the raw input:
   ```bash
   cat sample-data/document_01.txt | tr ' ' '\n' | sort | uniq -c | head -20
   ```
   The counts in your output won't match one file exactly (since all 20 files contribute), but the relative frequencies should make sense — high-frequency words like "the", "and", "in" should have the highest counts.
4. Test fault tolerance: while a job is running, kill one worker process (`kill -9 <pid>`). Observe the log showing `node_down` and task re-queuing. Verify the job still completes correctly.
5. Test throttling: before submitting a job, call `GenServer.cast(MrMaster.Master, {:set_throttle, :"worker1@localhost", 10.0})` from an `iex` session and observe that worker1's RPC calls are slower.

> **Review checkpoint:** Share your log output from a successful run, the fault tolerance test, and the spot-checked word counts.

---

## Commit Discipline

Commit after every task checkpoint passes review. Suggested message format:
```
feat(mr_protocol): add Task behaviour and data structs
feat(mr_master): add Job input splitting
feat(mr_master): add Scheduler locality algorithm
...
```

Keep `tmp/` and `output/` gitignored (already in `.gitignore`).
