# Multi-Machine Parametrization Plan (Option A: Worker Self-Registration)

**Date:** 2026-05-27
**Revised:** 2026-05-31 — reframed around worker self-registration after discovering the handshake already exists.
**Goal:** Run the MapReduce framework across multiple physical machines (e.g., 3 MacBook Pros on a LAN) with no changes to core MapReduce logic — only configuration, a job-start gate based on a minimum worker count, and a manual worker launch path.
**Approach:** The master is a passive rendezvous point. Workers are started (manually or via the existing local auto-spawn) knowing only the master's node name + cookie; they connect and register themselves. Parametrize the remaining hardcoded values (cookie, master node name, file paths) and add a minimum-worker gate plus a `mix mr.worker` launch task.

---

## Context: What Already Exists (Read This First)

This plan is mostly **subtraction and parametrization**, not new mechanism. The "Option A" discovery model — workers find the master, not vice versa — is already implemented:

- **Worker self-registers** (`apps/mr_worker/lib/mr_worker/worker.ex:61-107`): schedules `:connect_to_master`, calls `Node.connect(master_node)`, then `GenServer.call({MrMaster.Master, master_node}, {:register, %WorkerInfo{node, coords}})`, with exponential-backoff retry (up to 100 attempts). A worker needs only **master node name + cookie + its own coords**.
- **Master listens** (`apps/mr_master/lib/mr_master/master.ex:23,50-57`): `init` already calls `:net_kernel.monitor_nodes(true)`; the `{:register, worker_info}` handler adds the worker to `state.workers` and immediately calls `assign_pending_tasks/1`, so a worker that registers mid-job gets work right away.
- **Worker config already partly wired** (`config/config.exs:53-61`, `apps/mr_worker/lib/mr_worker/application.ex:11-21`): the worker reads `:mr_worker, :master_node` and `:mr_worker, :coords` from application env. The supervisor only starts the worker when `:mr_master, :start_master` is `false`.

What still ties the system to a single machine, all in `apps/mr_master/lib/mix/tasks/mr/start.ex`:

- It **spawns** worker OS processes itself via `Port.open` with hardcoded `worker#{n}@127.0.0.1` names (lines 89-101).
- The **cookie** `:secret` is hardcoded (line 57) and passed to spawned workers via `--cookie secret` (line 98).
- The **master node name** `master@127.0.0.1` is hardcoded (line 49); the worker's view of it is hardcoded in `config/config.exs:60`.
- The job-start gate waits for a **count equal to `--workers`** (line 103, `wait_for_workers/2` at 195-205).
- File paths are hardcoded: temp dir `tmp/#{worker_node_name}` (`map_task.ex:36`), output dir `"output/"` (`worker.ex:45`).

---

## Key Design Decisions

- **Discovery model:** Worker self-registration (already built). The master never connects out to workers; it accepts whoever registers. No static worker roster is required.
- **Job-start gate:** **Minimum worker count.** Config/CLI provides `min_workers`; the master waits until at least that many have registered, then submits the job. (Replaces the old "wait for exactly `--workers`" and the proposed static `worker_nodes` list.)
- **Two startup modes** in `mr.start`:
  - **Local dev (default, unchanged):** `--workers N` auto-spawns N local worker nodes and gates on that count.
  - **Distributed (`--distributed`):** skips spawning; waits for `min_workers` registrations.
- **Worker launch path:** a new `mix mr.worker` task to start a worker on any machine, reading master node / cookie / coords / temp dir from runtime config or CLI flags.
- **Coordinates:** worker uses coords from CLI/config if provided, else self-assigns random coords. (Random by default ≈ unknown placement; explicit when staging a locality demo.)
- **Cookie:** configurable at runtime (via `config/runtime.exs` + env var), documented as a prerequisite.
- **Intermediate (temp) files:** stay **worker-local** (paper-faithful); the temp base dir becomes configurable, defaulting to `tmp` so single-machine behavior is unchanged. Reduce workers continue to fetch buckets via the existing `MrWorker.FileServer`/RPC pull.
- **Final output:** the **master collects it via RPC.** A reduce worker, after computing its result, pushes the bytes to a master-side collector GenServer (mirroring `MrWorker.FileServer` in the opposite direction); the master writes every reducer's output into one directory on its own disk. No shared filesystem required.
  - *Known deviation from the paper (intentional):* in real MapReduce, reduce workers write output to GFS and the master never touches output data — funnelling output through the master makes it a data bottleneck. Acceptable at this project's scale; documented so it can be discussed as a deliberate trade-off (swap for shared/object storage to scale).
- **No changes to core MapReduce logic, the registration handshake, or fault-tolerance code.**

---

## Phase 1: Configuration Surface

### Task 1.1: Add runtime config for the values that vary per machine

**What to build:** Move per-machine / per-instance settings into `config/runtime.exs` so they can be set at startup via environment variables, without recompiling. Add the new `min_workers` and base-dir keys.

**Files to modify/create:**
- Create `config/runtime.exs` (currently absent) — read env vars at runtime.
- Modify `config/config.exs` (lines 53-61) — keep sane compile-time defaults; let runtime override.

**Details:**

The values that must vary per machine/instance: master node name, cookie, worker coords, temp base dir, output base dir, and `min_workers`. `config/config.exs` is evaluated at compile time, so it cannot give two workers on the same build different coords at runtime. `config/runtime.exs` runs at boot and is the correct home.

Sketch (`config/runtime.exs`):
```elixir
import Config

# Shared
cookie = System.get_env("MR_COOKIE", "secret") |> String.to_atom()
master_node = System.get_env("MR_MASTER_NODE", "master@127.0.0.1") |> String.to_atom()

# Master-only
config :mr_master,
  cookie: cookie,
  master_node: master_node,
  min_workers: String.to_integer(System.get_env("MR_MIN_WORKERS", "4")),
  output_base_dir: System.get_env("MR_OUTPUT_DIR", "output"),
  temp_base_dir: System.get_env("MR_TEMP_DIR", "tmp")

# Worker-only
config :mr_worker,
  cookie: cookie,
  master_node: master_node,
  temp_base_dir: System.get_env("MR_TEMP_DIR", "tmp")

# Coords: explicit MR_COORDS wins; otherwise leave unset so the worker self-assigns random (Task 3.2).
case System.get_env("MR_COORDS") do
  nil -> :ok
  str ->
    [x, y] = str |> String.split(",") |> Enum.map(&String.to_float/1)
    config :mr_worker, coords: {x, y}
end
```

Then simplify `config/config.exs:53-61` to compile-time defaults only (it can keep `start_master` and a default coords fallback, but the env-var parsing moves to runtime.exs).

**Why:** Per-machine values must be settable at boot on each machine without editing or recompiling code. Centralizing them in `runtime.exs` makes the config surface explicit and machine-portable.

---

### Task 1.2: Provide example configs and a config guide

**What to build:** Documentation and examples for single-machine and 3-machine setups.

**Files to create:**
- `config/multi_machine_example.md` — worked 3-machine example (env vars per machine).
- `config/README.md` — short guide to each env var and the two startup modes.

**Details:**

`config/multi_machine_example.md` should show the concrete commands, e.g.:
```
# macbook1.local — master + 1 worker
# macbook2.local — 2 workers
# macbook3.local — 2 workers   (min_workers = 5)

# On macbook1 (master):
MR_MASTER_NODE=master@macbook1.local MR_COOKIE=secret MR_MIN_WORKERS=5 \
  mix mr.start --distributed --input /path/to/input --reducers 4

# On each worker machine, once per worker (example: macbook2):
MR_MASTER_NODE=master@macbook1.local MR_COOKIE=secret MR_COORDS=80.0,20.0 \
  mix mr.worker --name worker1@macbook2.local
```

Document the env vars in a table: `MR_MASTER_NODE`, `MR_COOKIE`, `MR_MIN_WORKERS`, `MR_COORDS`, `MR_TEMP_DIR`, `MR_OUTPUT_DIR`.

**Why:** Gives a copy-pasteable template so a multi-machine run is reproducible.

---

## Phase 2: Master Startup

### Task 2.1: Use config for master node name and cookie

**What to build:** Replace the hardcoded master node name and cookie in `mr.start` with config lookups.

**Files to modify:**
- `apps/mr_master/lib/mix/tasks/mr/start.ex` (lines 49, 57)

**Details:**

Replace:
```elixir
case :net_kernel.start([:"master@127.0.0.1", :longnames]) do ...
Node.set_cookie(:secret)
```
With reads from `Application.fetch_env!(:mr_master, :master_node)` and `Application.fetch_env!(:mr_master, :cookie)`. (Optionally introduce a thin `MrMaster.Config` wrapper module if you prefer named accessors over `Application.fetch_env!` calls — not required.)

**Why:** Lets the master run under any node name and cookie on any machine.

---

### Task 2.2: Add `--distributed` mode and a `min_workers` gate

**What to build:** Keep the existing local auto-spawn as the default; add a `--distributed` flag that skips spawning and waits for `min_workers` registrations.

**Files to modify:**
- `apps/mr_master/lib/mix/tasks/mr/start.ex` (lines 14-17 option parsing; 89-110 spawn + gate)

**Details:**

Add `distributed: :boolean` (and optionally `min_workers: :integer` to override config) to the `OptionParser` strict list. Then branch the spawn/gate logic:

```elixir
expected =
  if options[:distributed] do
    min = options[:min_workers] || Application.fetch_env!(:mr_master, :min_workers)
    Logger.info("[master] Distributed mode — waiting for #{min} workers to register.")
    Logger.info("[master] Start workers on other machines with: mix mr.worker --name worker@host")
    min
  else
    # existing local behavior: spawn N workers, gate on N
    spawn_local_workers(options[:workers])   # the current Port.open loop, lines 89-101
    options[:workers]
  end

wait_for_workers(expected)
```

`wait_for_workers/2` (lines 195-205) already polls `:get_workers` until `map_size >= expected_count` — it needs no change; only its argument source changes. Keep the post-gate count check (lines 106-110). In distributed mode, `shutdown_workers/1` should still iterate `Node.list()` (it does) but there are no local `ports` to close — pass `[]`.

**Why:** Preserves the one-command local dev run while enabling manual multi-machine startup with a count-based readiness gate.

---

## Phase 3: Worker Launch Path

### Task 3.1: Add a `mix mr.worker` task

**What to build:** A Mix task that starts a single worker node on any machine. It starts Erlang distribution under the given name + cookie, then lets the existing `MrWorker.Application`/`MrWorker.Worker` registration handshake run.

**Files to create:**
- `apps/mr_worker/lib/mix/tasks/mr/worker.ex`

**Details:**

The task should:
1. Parse `--name` (required), optional `--coords "x,y"`, optional `--master`/`--cookie` overrides (else from config).
2. Start distribution: `:net_kernel.start([String.to_atom(name), :longnames])`; `Node.set_cookie(cookie)`.
3. Ensure `:mr_worker` is started (the supervisor starts `MrWorker.Worker` because `:mr_master, :start_master` is `false` on worker machines — see `application.ex:11`).
4. `Process.sleep(:infinity)` to keep the node alive (mirrors how `mr.start` stays up).

This replaces the documented raw `elixir --name ... -S mix run` invocation with one consistent entrypoint that matches how the master is started. Coords flow: `--coords` → set `:mr_worker, :coords` before the app starts; if absent, the worker self-assigns (Task 3.2).

**Why:** A single, discoverable launch command per worker, symmetric with `mix mr.start`, that carries the per-instance settings.

---

### Task 3.2: Worker self-assigns random coords when none configured

**What to build:** Make coords optional. If `:mr_worker, :coords` is unset, the worker generates random coords at startup instead of crashing on `fetch_env!`.

**Files to modify:**
- `apps/mr_worker/lib/mr_worker/application.ex` (lines 12-13) — or `worker.ex:7-8`

**Details:**

Replace `Application.fetch_env!(:mr_worker, :coords)` with a get-or-generate:
```elixir
coords =
  case Application.get_env(:mr_worker, :coords) do
    nil -> {Enum.random(0..99) * 1.0, Enum.random(0..99) * 1.0}
    c -> c
  end
```
This moves the random-coord logic that currently lives in the spawner (`mr.start.ex:93-95`) into the worker, so manually-launched workers behave the same.

**Why:** Option C — explicit coords when staging a locality demo, sensible random default otherwise. Keeps locality scheduling functional without per-worker setup.

---

## Phase 4: Output Collection & File Paths

### Task 4.1: Make the map temp base dir configurable (worker-local)

**What to build:** Use the configured `temp_base_dir` instead of the hardcoded `"tmp"`, defaulting to `tmp` so single-machine runs are unchanged. Temp stays worker-local — it is fetched via RPC, never shared.

**Files to modify:**
- `apps/mr_worker/lib/mr_worker/map_task.ex` (line 36)

**Details:**

Map temp dir (`map_task.ex:36` is currently `dirname = "tmp/#{worker_node_name}"`):
```elixir
base = Application.fetch_env!(:mr_worker, :temp_base_dir)   # default "tmp"
dirname = Path.join(base, worker_node_name)
```

No change to the reduce fetch path: reduce workers keep pulling buckets via `MrWorker.RPC.call(node, MrWorker.FileServer, {:fetch, path}, ...)`. The bucket path they request is whatever the map worker reported as its location, so a configurable temp dir flows through unchanged.

**Why:** Lets intermediate files live on a per-machine path (e.g. a fast local SSD or `/tmp`) without sharing them — preserving the paper-faithful "local disk + RPC fetch" model.

---

### Task 4.2: Collect reduce output at the master via RPC

**What to build:** A master-side collector GenServer that receives finished output files from reduce workers and writes them into one directory on the master's disk — mirroring `MrWorker.FileServer` in the opposite direction (push instead of pull). Reduce workers stop writing output to a local path and instead send the bytes to the master.

**Files to create/modify:**
- Create `apps/mr_master/lib/mr_master/output_collector.ex` — the collector GenServer.
- Modify `apps/mr_master/lib/mr_master/application.ex` — start the collector under the master's supervisor.
- Modify `apps/mr_worker/lib/mr_worker/reduce_task.ex` (writes output at `reduce_task.ex:34`) and its caller `apps/mr_worker/lib/mr_worker/worker.ex:45` — push the result to the master instead of writing locally.

**Details:**

The collector mirrors the existing `FileServer.handle_call({:fetch, path})` pattern. Sketch:
```elixir
defmodule MrMaster.OutputCollector do
  use GenServer
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_), do: {:ok, %{}}

  def handle_call({:write_output, filename, binary}, _from, state) do
    dir = Application.fetch_env!(:mr_master, :output_base_dir)   # default "output"
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, filename), binary)
    {:reply, :ok, state}
  end
end
```

Reduce side: today `reduce_task.ex` writes locally via `File.write` at `reduce_task.ex:34` (`output_dir`-based), and `worker.ex:45` passes the literal `"output/"`. Change `ReduceTask.execute` into a **pure function**: it computes the result and **returns `{filename, binary}`** — no filesystem write, no RPC, no master dependency. (Drop the `output_dir` parameter from its signature.) The **worker** owns the push, reusing the throttled RPC path so locality simulation still applies to the result transfer:
```elixir
# in worker.ex run_reduce, after ReduceTask.execute returns the {filename, binary}
MrWorker.RPC.call(master_node, MrMaster.OutputCollector,
  {:write_output, filename, binary}, registry, multiplier)
```
`filename` should be deterministic per reduce bucket (e.g. `"bucket-#{task.bucket}.txt"`) so reruns overwrite rather than duplicate. Output for word-count is small (aggregated counts), so a single binary per bucket is fine; if a future high-volume task needs it, chunk the transfer (the current `FileServer` ships whole files too, so this matches existing behavior).

**Why:** Produces one coherent output directory on the master with zero shared-filesystem setup, reusing the RPC file-transfer pattern already in the codebase. The master owns the output destination, as in the paper's coordinator model. Keeping `ReduceTask.execute` pure (returns `{filename, binary}`, no side effects) also makes it independently unit-testable with no master or filesystem — easier to test than the current write-then-read-back version.

**Local testing note:** This entire flow runs on a single machine — master and workers are separate Erlang nodes whether on one box or three, and RPC is location-transparent, so reduce output lands in the master's `output/` exactly as today. Validate locally first; the multi-machine run changes only node hostnames.

---

### Task 4.3: Document output-collection and filesystem options

**What to build:** A short guide covering where output lands and the (optional) alternatives.

**Files to create:**
- `docs/MULTI_MACHINE_SETUP.md` (output/filesystem section; full guide completed in Task 5.2)

**Details:** Document, in order of recommendation:
1. **Master-collects via RPC (default — Task 4.2):** no shared filesystem needed; final output appears in `output_base_dir` on the master. Note the deliberate paper deviation (master as data funnel) and when you'd outgrow it.
2. **Shared output dir (NFS), optional:** if you'd rather be GFS-faithful, point `output_base_dir` at an NFS/`/Volumes/shared` mount and have reducers write there directly instead of pushing to the master. More setup; removes the master bottleneck.
3. **Temp stays local in all cases:** intermediate files are RPC-fetched and must **not** be shared — sharing them bypasses the paper-faithful fetch.

**Why:** Makes the output story explicit and records the trade-off between the simple default and the more scalable shared-FS variant.

---

## Phase 5: Testing & Documentation

### Task 5.1: Test the min-workers gate and self-registration

**What to build:** A test verifying the master begins only after `min_workers` register, and that a worker registering after job submission still receives pending tasks (the existing `assign_pending_tasks` path).

**Files to create:**
- `apps/mr_master/test/mr_master/multi_machine_test.exs`

**Details:**

Drive the master GenServer directly (no real nodes needed):
```elixir
test "registering a worker mid-job assigns pending tasks" do
  # start master, submit a job with pending map tasks and zero workers
  # assert phase is :mapping with tasks unassigned
  GenServer.call(MrMaster.Master, {:register, %MrProtocol.WorkerInfo{node: :"w1@host", coords: {1.0, 1.0}}})
  # assert the worker now has an assigned task
end
```
Mirror the existing master test style/helpers. Also assert `wait_for_workers/2`'s gate logic against a simulated registration count if it is extracted testably.

**Why:** Locks in the two behaviors the whole "Option A" model rests on: the readiness gate and mid-job join.

---

### Task 5.2: Write `MULTI_MACHINE_SETUP.md`

**What to build:** Step-by-step guide for a 3-MacBook run.

**Files to modify/create:**
- `docs/MULTI_MACHINE_SETUP.md` (complete it)

**Details:** Outline:
1. Prerequisites (same Elixir/Erlang on all machines; same git checkout/build).
2. Network: `.local` mDNS names; verify `ping macbook2.local`.
3. Cookie: all nodes share `MR_COOKIE`.
4. Output/filesystem: choose option from Task 4.3 (recommend master-collects via RPC — no shared FS).
5. Launch: start workers with `mix mr.worker` on macbook2/3; start master with `mix mr.start --distributed` on macbook1; watch logs for registrations and `=== MapReduce Job Complete ===`.
6. Troubleshooting: cookie mismatch (`Node.connect` returns false), name-in-use in epmd, firewall blocking epmd port 4369 / distribution ports.

**Why:** Makes a multi-machine run reproducible end to end.

---

## Review Checkpoints

1. **Phase 1:** Config review — every per-machine value reads from `runtime.exs`/env; defaults preserve single-machine behavior.
2. **Phase 2:** Local run still works one-command (`mix mr.start`); `--distributed` waits for `min_workers` and starts on registration.
3. **Phase 3:** `mix mr.worker` brings up a worker that registers; manual two-node test (one master, one worker) on localhost, then across two machines.
4. **Phase 4:** Temp files land under `temp_base_dir` (worker-local) and reduce still fetches via RPC; reduce workers push results to `MrMaster.OutputCollector` and all output appears in one directory on the master under `output_base_dir`.
5. **Phase 5:** Tests pass; full word-count job on 3 MacBooks produces output identical to a single-machine run.

---

## Implementation Order

1. Task 1.1 — runtime config surface
2. Task 1.2 — example config + guide
3. Task 2.1 — master node name + cookie from config
4. Task 2.2 — `--distributed` mode + `min_workers` gate
5. Task 3.1 — `mix mr.worker` launch task
6. Task 3.2 — worker self-assigns random coords
7. Task 4.1 — configurable worker-local temp base dir
8. Task 4.2 — master output collector + reduce push via RPC
9. Task 4.3 — output/filesystem options doc
10. Task 5.1 — gate + mid-job-join test
11. Task 5.2 — `MULTI_MACHINE_SETUP.md`

Submit each task for review before proceeding.

---

## Non-Goals

- **Automated remote worker spawning** (master SSHing into machines to launch BEAMs). Manual launch via `mix mr.worker` is simpler and more transparent. (This is the deferred alternative to Option A; Option B / libcluster gossip discovery is a possible future upgrade.)
- **Broadcast discovery** (libcluster gossip, mDNS service browsing). Workers are pointed at a known master node name; that is sufficient for a LAN of a few machines.
- **Dynamic master failover / multi-master.** Single master, as in the paper.
- **Multi-datacenter replication.** Local network assumed.
