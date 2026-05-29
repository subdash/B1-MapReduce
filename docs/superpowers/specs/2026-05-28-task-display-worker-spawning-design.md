# Design: Task Type Display + Worker Spawning

**Date:** 2026-05-28  
**Status:** Design complete, ready for implementation planning

---

## Overview

Two UI enhancements to the MapReduce dashboard:

1. **Display the current task type** (e.g., "Word Count", "Distributed Grep") in the header alongside elapsed time
2. **Allow spinning up new workers on demand** via an "Add Worker" button that runs a Mix task

Both features enhance observability and cluster flexibility during development and testing.

---

## Feature 1: Display Task Type in Header

### Problem
The dashboard currently shows elapsed time, master connection status, and job phase, but doesn't display what task is running. Users must check logs or remember which task was submitted.

### Solution
Extract the task type from the master's state and render it prominently in the header.

### Architecture

**Data flow:**
- Master already tracks `task_module` (e.g., `MrWorker.Tasks.WordCount`)
- Master includes it in `:get_state` response to dashboard
- Dashboard extracts and derives human-readable name
- Dashboard renders in header

**Task name formatting:**
- Input: `MrWorker.Tasks.WordCount` (atom)
- Process: Split module path, take last segment, convert to title case
- Output: `"Word Count"`
- Unknown/nil: `"Unknown"`

### UI Placement
In the header, between "Master connected" indicator and elapsed time timer. Group task type with other metadata (master status, elapsed time).

### Implementation Details

**Dashboard LiveView** (`dashboard_live.ex`):
- Add `:task_type` to socket assigns in `mount/3` and `assign_master_state/2`
- Extract `task_module` from master state and format it
- Create helper function `format_task_name(task_module)` to convert atom to title case string

**Template** (`dashboard_live.html.heex`):
- Add task type display in header
- Format: simple text label, e.g., "Task: Word Count"

**Edge cases:**
- Master not connected: show `"—"` or `"Waiting for master"`
- Job not started: don't display task type until job begins
- Unknown module: display `"Unknown"` and log a warning

---

## Feature 2: Spawn Worker via Mix Task

### Problem
Currently, workers can only be added at startup via the CLI `--workers` flag. Testing fault tolerance and dynamic scaling requires manual setup outside the UI.

### Solution
Provide a Mix task (`mix mr.spawn_worker`) that the dashboard can invoke to spawn a new worker on demand. The worker auto-registers with the master via the existing registration mechanism.

### Architecture

**New Mix task:** `Mix.Tasks.Mr.SpawnWorker`

**Inputs:**
- `--master` (optional, default: `master@127.0.0.1`) — master node to register with
- `--coords` (optional, default: random) — coordinates for the worker on the 2D grid
  - Format: `x,y` where x, y are floats in range [0, 100]
  - If omitted, generate random coordinates like startup workers

**Behavior:**
1. Validate master node is reachable (best-effort; fail gracefully if not)
2. Generate a unique worker node name: `worker_<timestamp>@127.0.0.1`
   - Use millisecond precision to avoid collisions
   - Format: `worker_1716936000123@127.0.0.1`
3. Spawn worker as a subprocess via `Port.open()`, passing:
   - Node name
   - Cookie (`:secret`, hardcoded to match master)
   - Coordinates
   - Master node location
4. Wait briefly for worker to register with master (best-effort)
5. Output worker node name to stdout for confirmation
6. Exit with code 0 on success, non-zero on failure
7. Log events: worker spawned, registration detected

**Implementation details:**

- Location: `apps/mr_master/lib/mix/tasks/mr/spawn_worker.ex`
- Reuse existing worker spawning logic from `mr.start` task
- Extract common spawning code into a helper module if needed to avoid duplication
- Shell command pattern (from `mr.start`):
  ```
  sh -c 'MR_START_MASTER=false MR_COORDS=x,y elixir --name worker_NNN@127.0.0.1 --cookie secret -S mix run --no-halt'
  ```

**Dashboard side** (`dashboard_live.ex`):
- Add "Add Worker" button in worker header (next to "Worker Nodes (N registered)")
- On click:
  - Call `System.cmd("mix", ["mr.spawn_worker", "--master", "master@127.0.0.1"], [])`
  - On success: no immediate feedback needed (polling loop detects new worker ~100ms later)
  - On error: show error toast/notification to user
  - Debounce rapid clicks (optional; single-user dashboard, so not critical)

**Edge cases & error handling:**

| Case | Behavior |
|------|----------|
| Master unreachable | Task logs warning, returns exit code 1, dashboard shows error toast |
| Node name collision | Use timestamp to avoid; if collision occurs, retry with new timestamp |
| Worker fails to register | Task logs warning, still exits 0 (worker may register later) |
| Rapid button clicks | Each spawns a separate worker (no debounce, but ok for development) |
| Worker process dies immediately | Master detects via `nodedown`, no user action needed |
| Coordinates out of range | Clamp to [0, 100] or reject with error message |

---

## Data Flow Diagrams

### Feature 1: Task Type Display
```
Master.state.task_module
    ↓ (via :get_state)
DashboardLive polls master
    ↓
assign_master_state/2 extracts task_module
    ↓
format_task_name/1 converts to "Word Count"
    ↓
Dashboard renders in header
```

### Feature 2: Worker Spawning
```
User clicks "Add Worker"
    ↓
Dashboard calls System.cmd("mix mr.spawn_worker")
    ↓
Mix task validates master, generates node name
    ↓
Spawns worker via Port.open()
    ↓
Worker node starts, auto-registers with master
    ↓
Master receives register call, updates state
    ↓
Dashboard polling loop detects new worker (~100ms)
    ↓
New worker appears on dashboard
```

---

## Testing

### Feature 1: Task Type Display
- [ ] Task type displays correctly for WordCount job
- [ ] Task type displays correctly for DistributedGrep job
- [ ] Unknown task module formatted gracefully
- [ ] Master disconnection handled (display "—" or "Waiting")

### Feature 2: Worker Spawning
- [ ] `mix mr.spawn_worker` spawns a worker with random coords
- [ ] `mix mr.spawn_worker --coords 10,20` spawns with specified coords
- [ ] Spawned worker appears on dashboard within 1 second
- [ ] Spawned worker can be assigned tasks
- [ ] Multiple spawns work correctly (no name collisions)
- [ ] Error toast shows on dashboard if spawn fails
- [ ] Spawned worker can be killed via dashboard (existing kill functionality)

---

## Implementation Order

1. **Feature 1** (simpler, no new backend code)
   - Update `dashboard_live.ex` to extract task_module
   - Update template to display task type
   - Test with running job

2. **Feature 2** (requires new Mix task)
   - Create `Mix.Tasks.Mr.SpawnWorker`
   - Test task manually via CLI
   - Add button and handler to `dashboard_live.ex`
   - Test e2e: click button, worker appears

---

## Future Enhancements (out of scope)

- Persist worker spawning history
- Custom worker naming scheme
- Spawn workers with specific task type affinity
- Bulk spawn (N workers at once)
- Kill all workers button
- Worker resource limits (CPU, memory) via cgroups or similar
