# Multi-Machine Parametrization Plan

**Date:** 2026-05-27  
**Goal:** Enable the MapReduce framework to run on multiple physical machines (e.g., 3 MacBook Pros on a LAN) without code changes—only configuration and manual node startup.  
**Approach:** Parametrize hardcoded values (node names, file paths, cookie) so nodes can be started on different machines with a shared configuration.

---

## Key Design Decisions

- **Node startup:** Manual SSH + mix task on each machine (not automated spawning from master). The master only connects to nodes that are already running.
- **File paths:** All relative paths become absolute or relative to a configurable base directory per machine.
- **Cookie:** Configurable at runtime, documented as a prerequisite.
- **Input/Output/Intermediate data:** Either on NFS/shared filesystem, or specified with absolute paths. Users choose which suits their setup.
- **No code changes to core MapReduce logic:** All changes are parametrization in config and startup helpers.

---

## Phase 1: Configuration Infrastructure

### Task 1.1: Create config modules for multi-machine setup

**What to build:** A new config module that holds all the parameters that vary per machine/deployment.

**Files to create/modify:**
- Create `apps/mr_master/lib/mr_master/config.ex` — configuration module for master
- Create `apps/mr_worker/lib/mr_worker/config.ex` — configuration module for worker
- Modify `apps/mr_master/lib/mix/tasks/mr/start.ex` — pass config instead of hardcoded values

**Details:**

`MrMaster.Config` should expose:
```elixir
def master_node_name(), do: Application.fetch_env!(:mr_master, :node_name)
def worker_nodes(), do: Application.fetch_env!(:mr_master, :worker_nodes)
def erlang_cookie(), do: Application.fetch_env!(:mr_master, :cookie)
def output_base_dir(), do: Application.fetch_env!(:mr_master, :output_base_dir)
def temp_base_dir(), do: Application.fetch_env!(:mr_master, :temp_base_dir)
```

`MrWorker.Config` should expose:
```elixir
def master_node_name(), do: Application.fetch_env!(:mr_worker, :master_node)
def erlang_cookie(), do: Application.fetch_env!(:mr_worker, :cookie)
def temp_base_dir(), do: Application.fetch_env!(:mr_worker, :temp_base_dir)
def my_coords(), do: Application.fetch_env!(:mr_worker, :coords)
```

**Why:** Centralizes config logic so it's easy to test, validate, and reason about. Makes it clear what the system needs to know.

---

### Task 1.2: Create config files for local (single-machine) and multi-machine deployments

**What to build:** Example `config.exs` files showing how to configure for single-machine (current) and multi-machine (3-node) setups.

**Files to create/modify:**
- `config/config.exs` — root config (unchanged)
- `config/dev.exs` — single-machine dev config (add examples)
- Create `config/multi_machine_example.exs` — documentation and example for 3-machine setup
- Create `config/README.md` — guide explaining how to use each config

**Details:**

`config/dev.exs` should include comments explaining the single-machine layout:
```elixir
config :mr_master,
  node_name: :"master@127.0.0.1",
  worker_nodes: [:"worker1@127.0.0.1", :"worker2@127.0.0.1", :"worker3@127.0.0.1"],
  cookie: :secret,
  output_base_dir: "output",
  temp_base_dir: "tmp"
```

`config/multi_machine_example.exs` (documentation):
```elixir
# Example: 3 MacBook Pros on a LAN
# macbook1.local (master + 1 worker)
# macbook2.local (2 workers)
# macbook3.local (2 workers)

# On macbook1, start master with:
#   iex --name master@macbook1.local --cookie secret -S mix run
#
# On each machine, start workers with (example for macbook2):
#   elixir --name worker1@macbook2.local --cookie secret -S mix run

# Master config (on macbook1):
config :mr_master,
  node_name: :"master@macbook1.local",
  worker_nodes: [
    :"worker1@macbook1.local",
    :"worker2@macbook2.local",
    :"worker3@macbook2.local",
    :"worker4@macbook3.local",
    :"worker5@macbook3.local"
  ],
  cookie: :secret,
  output_base_dir: "/Users/shared/mapreduce/output",  # NFS mount or shared path
  temp_base_dir: "/Users/shared/mapreduce/tmp"
```

**Why:** Provides a clear template and documentation so users know exactly how to set up their multi-machine cluster.

---

## Phase 2: Update Master Startup

### Task 2.1: Refactor mr.start task to use config instead of spawning workers

**What to build:** Remove the `Port.open` worker spawning; instead, wait for workers to connect.

**Files to modify:**
- `apps/mr_master/lib/mix/tasks/mr/start.ex`

**Details:**

Current flow (lines 68-79):
```elixir
Enum.each(1..workers, fn worker_num ->
  worker_name = "worker#{worker_num}@127.0.0.1"
  ...
  Port.open({:spawn, command}, [])
end)
```

New flow:
1. Read expected worker nodes from config: `MrMaster.Config.worker_nodes()`
2. Log instructions for user: "Waiting for workers: [worker1@macbook2.local, ...]"
3. Wait for all expected workers to connect (existing `wait_for_workers` logic)
4. No changes to the rest of the task

```elixir
expected_workers = MrMaster.Config.worker_nodes()
Logger.info("[master] Waiting for #{length(expected_workers)} workers to connect...")
Logger.info("[master] Start workers manually on other machines with:")
Logger.info("[master]   elixir --name worker1@<hostname> --cookie secret -S mix run")

wait_for_workers(length(expected_workers))
```

**Why:** Makes it explicit that workers must be started separately, and gives the user clear instructions.

---

### Task 2.2: Update master node startup to use config

**What to build:** Use `MrMaster.Config.master_node_name()` and `MrMaster.Config.erlang_cookie()` instead of hardcoded values.

**Files to modify:**
- `apps/mr_master/lib/mix/tasks/mr/start.ex` (lines 49, 57)

**Details:**

Replace:
```elixir
case :net_kernel.start([:"master@127.0.0.1", :longnames]) do
  ...
end

Node.set_cookie(:secret)
```

With:
```elixir
master_name = MrMaster.Config.master_node_name()
cookie = MrMaster.Config.erlang_cookie()

case :net_kernel.start([master_name, :longnames]) do
  ...
end

Node.set_cookie(cookie)
```

**Why:** Allows master to run on any node name.

---

## Phase 3: Update File Paths

### Task 3.1: Make temp_base_dir and output_base_dir absolute paths

**What to build:** Update master and worker to use configurable base directories for intermediate and output files.

**Files to modify:**
- `apps/mr_worker/lib/mr_worker/map_task.ex` (line 36)
- `apps/mr_worker/lib/mr_worker/reduce_task.ex` (line 34)
- `apps/mr_master/lib/mr_master/master.ex` — pass base dirs to workers in task messages

**Details:**

Current (map_task.ex:36):
```elixir
dirname = "tmp/#{worker_node_name}"
```

New:
```elixir
base_dir = MrWorker.Config.temp_base_dir()
dirname = Path.join(base_dir, "#{worker_node_name}")
```

For reduce_task.ex (line 2-3), accept base_dir as parameter:
```elixir
def execute(task, task_module, registry, base_dir, throttle_multiplier) do
  ...
  file_path = "#{base_dir}/bucket-#{task.bucket}.txt"
```

Master must pass base_dir when calling reduce:
```elixir
output_base_dir = MrMaster.Config.output_base_dir()
MrWorker.ReduceTask.execute(task, task_module, registry, output_base_dir, throttle)
```

**Why:** Allows intermediate files and output to live on a shared NFS mount or different filesystem per machine.

---

### Task 3.2: Document filesystem setup options

**What to build:** A guide explaining how to set up shared filesystem access.

**Files to create:**
- Create `docs/MULTI_MACHINE_SETUP.md`

**Details:**

Document three options:
1. **NFS (recommended):** Mount shared directory on all machines at the same path.
   ```bash
   # On master (macbook1):
   # Configure /Users/shared/mapreduce as NFS export
   
   # On workers (macbook2, macbook3):
   # Mount NFS: mount_nfs macbook1.local:/Users/shared/mapreduce /Users/shared/mapreduce
   ```

2. **Local tmp, centralized output:** Each machine has local `tmp/` for intermediate files, but `output/` is on NFS.
   ```
   config :mr_worker,
     temp_base_dir: "/tmp/mapreduce",
     output_base_dir: "/Volumes/shared/mapreduce/output"
   ```

3. **Rsync after completion:** Manual sync of output files after job completion (not recommended for large jobs).

**Why:** Gives users clear guidance on filesystem setup without code changes.

---

## Phase 4: Update Master Startup to Handle Multi-Machine Job Submission

### Task 4.1: Update mr.start task to accept worker node list

**What to build:** Allow users to override `worker_nodes` from config via command-line args.

**Files to modify:**
- `apps/mr_master/lib/mix/tasks/mr/start.ex` (add --workers arg parsing)

**Details:**

Current approach: Config specifies workers.

Enhanced approach: Allow CLI override:
```elixir
# Read from config
expected_workers = MrMaster.Config.worker_nodes()

# Or override via CLI
{parsed, _argv, _errors} = OptionParser.parse(args, 
  strict: [workers: :string, ...]  # "worker1@mac1.local,worker2@mac2.local"
)

expected_workers = 
  case Keyword.fetch(parsed, :workers) do
    {:ok, worker_string} ->
      worker_string
      |> String.split(",")
      |> Enum.map(&String.to_atom/1)
    :error ->
      MrMaster.Config.worker_nodes()
  end
```

**Why:** Makes it easy to test different cluster topologies without editing config files.

---

## Phase 5: Testing & Documentation

### Task 5.1: Add integration test for multi-machine startup

**What to build:** A test that verifies nodes can be connected and a job submitted without local worker spawning.

**Files to create:**
- `apps/mr_master/test/mr_master/multi_machine_test.exs`

**Details:**

```elixir
defmodule MrMaster.MultiMachineTest do
  use ExUnit.Case

  test "master waits for manually-started workers" do
    # Simulate a manually-started worker connecting
    :ok = GenServer.call(MrMaster.Master, {:register, %WorkerInfo{node: :worker1@macbook2, ...}})
    
    # Verify worker is registered
    workers = GenServer.call(MrMaster.Master, :get_workers)
    assert Map.has_key?(workers, :worker1@macbook2)
  end
end
```

**Why:** Ensures the startup flow works for manual worker registration.

---

### Task 5.2: Create MULTI_MACHINE_SETUP.md with step-by-step instructions

**What to build:** A detailed guide for setting up and running the system on 3 physical MacBooks.

**Files to create:**
- `docs/MULTI_MACHINE_SETUP.md`

**Details:**

Outline:
1. Prerequisites (Erlang, Elixir installed on all machines)
2. Filesystem setup (NFS or alternative)
3. Network setup (mDNS or hosts file)
4. Example: 3-machine deployment
   - Step 1: Prepare config on macbook1
   - Step 2: Share code to all machines (git clone)
   - Step 3: Start workers on macbook2 and macbook3
   - Step 4: Start master on macbook1 with `mix mr.start --input /path/to/input --workers ...`
   - Step 5: Monitor job completion via logs
5. Troubleshooting (node not found, cookie mismatch, etc.)

**Why:** Makes it reproducible. Future you or another developer can follow this exactly.

---

## Review Checkpoints

After each phase:

1. **Phase 1 complete:** Code review of config modules. Check that all hardcoded values are now config-driven.
2. **Phase 2 complete:** Manual test on a single machine with localhost; then on two machines (one master, one worker) to verify node connection.
3. **Phase 3 complete:** Verify intermediate and output files are written to configured paths.
4. **Phase 4 complete:** Test CLI override of worker nodes.
5. **Phase 5 complete:** Run full word-count job on 3 physical MacBooks and verify output matches single-machine run.

---

## Implementation Order

1. Task 1.1 — Config modules
2. Task 1.2 — Config files + examples
3. Task 2.1 — Remove worker spawning from mr.start
4. Task 2.2 — Use config for node names and cookie
5. Task 3.1 — Parametrize file paths
6. Task 3.2 — Document filesystem options
7. Task 4.1 — CLI override for workers
8. Task 5.1 — Integration test
9. Task 5.2 — MULTI_MACHINE_SETUP.md

Each task should be submitted for review before proceeding to the next.

---

## Non-Goals (Phase 2)

- Automated worker spawning on remote machines (SSH, etc.) — manual startup is simpler and more transparent.
- Dynamic worker discovery — nodes must be specified upfront.
- Multi-datacenter replication — local network assumed.
