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
- **File paths:** temp and output base dirs become configurable; default to current relative paths so single-machine behavior is unchanged.
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

# Coords: explicit MR_COORDS wins; otherwise leave unset so the worker self-assigns random (Task 4.2).
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

## Phase 4: File Paths

### Task 4.1: Make temp and output base dirs configurable

**What to build:** Use the configured `temp_base_dir` / `output_base_dir` instead of hardcoded `"tmp"` / `"output/"`, defaulting to the current values so single-machine runs are unchanged.

**Files to modify:**
- `apps/mr_worker/lib/mr_worker/map_task.ex` (line 36)
- `apps/mr_worker/lib/mr_worker/worker.ex` (line 45) and the reduce-task call path
- `apps/mr_master/lib/mr_master/master.ex` — pass `output_base_dir` into reduce task messages if the master owns it; otherwise the worker reads it from its own config

**Details:**

Map temp dir (`map_task.ex:36`):
```elixir
base = Application.fetch_env!(:mr_worker, :temp_base_dir)   # default "tmp"
dirname = Path.join(base, worker_node_name)
```

Reduce output: `reduce_task.ex:2` already takes `output_dir` as a parameter; `worker.ex:45` passes the literal `"output/"`. Replace that literal with the configured output base dir. Decide ownership: simplest is the worker reading `:mr_worker, :temp_base_dir` locally and the output dir coming from config too. If output must live on a single shared location chosen by the master, pass `output_base_dir` in the `:run_reduce` cast payload instead.

**Why:** Lets intermediate and final files live on a shared NFS mount or a per-machine path without touching task logic.

---

### Task 4.2: Document filesystem options

**What to build:** A guide covering how intermediate/output data is shared across machines.

**Files to create:**
- `docs/MULTI_MACHINE_SETUP.md` (filesystem section; full guide completed in Task 5.2)

**Details:** Document three options:
1. **Local temp + shared output (recommended for this project):** each machine keeps `tmp/` local; `output/` points at an NFS/`/Volumes/shared` path. Matches the paper's "intermediate on local disk, fetched via RPC" model — reduce workers already fetch buckets via `MrWorker.FileServer`/RPC, so temp does **not** need to be shared.
2. **Fully shared (NFS):** everything under one mounted path. Simplest mental model, slowest.
3. **Rsync after completion:** manual, not recommended for large jobs.

**Why:** Note specifically that **temp dirs should stay local** — the RPC fetch is the paper-faithful path and sharing temp would bypass it. Only output benefits from a shared location.

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
4. Filesystem: choose option from Task 4.2 (recommend local temp + shared output).
5. Launch: start workers with `mix mr.worker` on macbook2/3; start master with `mix mr.start --distributed` on macbook1; watch logs for registrations and `=== MapReduce Job Complete ===`.
6. Troubleshooting: cookie mismatch (`Node.connect` returns false), name-in-use in epmd, firewall blocking epmd port 4369 / distribution ports.

**Why:** Makes a multi-machine run reproducible end to end.

---

## Review Checkpoints

1. **Phase 1:** Config review — every per-machine value reads from `runtime.exs`/env; defaults preserve single-machine behavior.
2. **Phase 2:** Local run still works one-command (`mix mr.start`); `--distributed` waits for `min_workers` and starts on registration.
3. **Phase 3:** `mix mr.worker` brings up a worker that registers; manual two-node test (one master, one worker) on localhost, then across two machines.
4. **Phase 4:** Temp files land under `temp_base_dir`, output under `output_base_dir`; confirm temp stays local and reduce still fetches via RPC.
5. **Phase 5:** Tests pass; full word-count job on 3 MacBooks produces output identical to a single-machine run.

---

## Implementation Order

1. Task 1.1 — runtime config surface
2. Task 1.2 — example config + guide
3. Task 2.1 — master node name + cookie from config
4. Task 2.2 — `--distributed` mode + `min_workers` gate
5. Task 3.1 — `mix mr.worker` launch task
6. Task 3.2 — worker self-assigns random coords
7. Task 4.1 — configurable temp/output base dirs
8. Task 4.2 — filesystem options doc
9. Task 5.1 — gate + mid-job-join test
10. Task 5.2 — `MULTI_MACHINE_SETUP.md`

Submit each task for review before proceeding.

---

## Non-Goals

- **Automated remote worker spawning** (master SSHing into machines to launch BEAMs). Manual launch via `mix mr.worker` is simpler and more transparent. (This is the deferred alternative to Option A; Option B / libcluster gossip discovery is a possible future upgrade.)
- **Broadcast discovery** (libcluster gossip, mDNS service browsing). Workers are pointed at a known master node name; that is sufficient for a LAN of a few machines.
- **Dynamic master failover / multi-master.** Single master, as in the paper.
- **Multi-datacenter replication.** Local network assumed.
