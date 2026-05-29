# Task Display & Worker Spawning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add task type display to dashboard header and implement a Mix task to spawn workers on demand via the dashboard UI.

**Architecture:** Feature 1 extends the dashboard polling to extract and display `task_module` from master state. Feature 2 creates a new Mix task that spawns worker nodes via `Port.open()` and adds a button to the dashboard to invoke it, showing errors via toast notifications.

**Tech Stack:** Elixir/OTP, Phoenix LiveView, Mix tasks

---

## Feature 1: Display Task Type in Header

### Task 1: Add `:task_type` to socket assigns and extract from master state

**Files:**
- Modify: `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex`

- [ ] **Step 1: Add `:task_type` to initial socket assigns in `mount/3`**

In `dashboard_live.ex`, update the `mount/3` function to initialize the `:task_type` assign before calling `assign_master_state`:

```elixir
@impl true
def mount(_params, _session, socket) do
  # Initialise UI-only state before the first data fetch so assign_master_state
  # never has to worry about these keys not existing.
  socket = assign(socket, selected_worker: nil, confirm_kill: nil, final_elapsed_time: nil, task_type: nil)

  if connected?(socket) do
    Process.send_after(self(), :update_state, @poll_interval_ms)
  end

  {:ok, assign_master_state(socket, fetch_master_state())}
end
```

- [ ] **Step 2: Extract `task_module` and format it in `assign_master_state/2`**

Update `assign_master_state/2` to extract the task module from master state and convert it to a human-readable task type:

```elixir
defp assign_master_state(socket, state) do
  job_complete = state.phase == :done

  # Capture elapsed time when job completes, then freeze it
  final_elapsed = if job_complete and socket.assigns[:final_elapsed_time] == nil do
    format_elapsed_ms(state.start_time)
  else
    socket.assigns[:final_elapsed_time]
  end

  elapsed_time = final_elapsed || format_elapsed_ms(state.start_time)

  # Extract and format task type
  task_type = if state.task_module, do: format_task_name(state.task_module), else: nil

  assign(socket,
    master_connected: true,
    workers: state.workers,
    map_tasks: state.map_tasks,
    reduce_tasks: state.reduce_tasks,
    phase: state.phase,
    recent_events: Map.get(state, :recent_events, []),
    worker_memory: fetch_worker_memory(state.workers),
    elapsed_time: elapsed_time,
    job_complete: job_complete,
    final_elapsed_time: final_elapsed,
    task_type: task_type
  )
end
```

- [ ] **Step 3: Update `assign_master_state/2` nil case to initialize `:task_type`**

Update the nil case (master not connected) to include `:task_type`:

```elixir
defp assign_master_state(socket, nil) do
  assign(socket,
    master_connected: false,
    workers: %{},
    map_tasks: %{},
    reduce_tasks: %{},
    phase: :waiting,
    recent_events: [],
    worker_memory: %{},
    elapsed_time: nil,
    job_complete: false,
    final_elapsed_time: nil,
    task_type: nil
  )
end
```

- [ ] **Step 4: Create `format_task_name/1` helper function**

Add this helper function at the bottom of `dashboard_live.ex`, after the other helper functions:

```elixir
# Convert task module atom to human-readable task name.
# Examples:
#   MrWorker.Tasks.WordCount -> "Word Count"
#   MrWorker.Tasks.DistributedGrep -> "Distributed Grep"
#   nil -> "Unknown"
defp format_task_name(nil), do: "Unknown"

defp format_task_name(task_module) do
  task_module
  |> Atom.to_string()
  |> String.split(".")
  |> List.last()
  |> String.replace(~r/([a-z])([A-Z])/, "\\1 \\2")
  |> String.replace(~r/([a-z])([A-Z])/, "\\1 \\2")
end
```

- [ ] **Step 5: Test the helper locally**

Add a quick test by running this in `iex`:

```elixir
# In iex -S mix phx.server (from mr_dashboard dir)
MrDashboardWeb.DashboardLive.format_task_name(MrWorker.Tasks.WordCount)
# => "Word Count"
MrDashboardWeb.DashboardLive.format_task_name(MrWorker.Tasks.DistributedGrep)
# => "Distributed Grep"
MrDashboardWeb.DashboardLive.format_task_name(nil)
# => "Unknown"
```

- [ ] **Step 6: Commit**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
git add apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex
git commit -m "feat: extract and assign task_type from master state"
```

### Task 2: Display task type in dashboard header

**Files:**
- Modify: `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex`

- [ ] **Step 1: Add task type display to header**

In `dashboard_live.html.heex`, update the header section to include the task type. Find this block (around line 8):

```heex
<div class="flex items-center gap-6 text-sm">
  <div class="flex items-center gap-3">
    <%= if @master_connected do %>
      <span class="flex items-center gap-1.5 text-green-400">
        <span class="w-2 h-2 rounded-full bg-green-400 animate-pulse"></span>
        Master connected
      </span>
    <% else %>
      <span class="flex items-center gap-1.5 text-yellow-400">
        <span class="w-2 h-2 rounded-full bg-yellow-400"></span>
        Waiting for master…
      </span>
    <% end %>
  </div>
```

Replace it with:

```heex
<div class="flex items-center gap-6 text-sm">
  <div class="flex items-center gap-3">
    <%= if @master_connected do %>
      <span class="flex items-center gap-1.5 text-green-400">
        <span class="w-2 h-2 rounded-full bg-green-400 animate-pulse"></span>
        Master connected
      </span>
    <% else %>
      <span class="flex items-center gap-1.5 text-yellow-400">
        <span class="w-2 h-2 rounded-full bg-yellow-400"></span>
        Waiting for master…
      </span>
    <% end %>
  </div>

  <%= if @task_type do %>
    <div class="flex items-center gap-2">
      <span class="px-2.5 py-1 rounded-full font-mono bg-blue-900 text-blue-200 text-xs">
        <%= @task_type %>
      </span>
    </div>
  <% end %>
```

This displays the task type as a blue badge between the master status and elapsed time.

- [ ] **Step 2: Commit**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
git add apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex
git commit -m "feat: display task type in dashboard header"
```

### Task 3: Manual test of task type display

**Files:**
- No files created/modified (testing only)

- [ ] **Step 1: Start master with word count task**

In one terminal, from the root directory:

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
mix mr.start --task word_count --workers 2 --reducers 2
```

- [ ] **Step 2: Open dashboard in browser**

Navigate to `http://localhost:4000` and verify:
- Header shows "Word Count" in a blue badge
- Badge appears between "Master connected" indicator and elapsed time

- [ ] **Step 3: Verify with DistributedGrep task**

Stop the previous run (Ctrl+C), then run:

```bash
mix mr.start --task distributed_grep --workers 2 --reducers 2
```

Verify the dashboard now shows "Distributed Grep" in the header.

- [ ] **Step 4: Note for committer**

No commit needed for this task — it's manual verification only.

---

## Feature 2: Spawn Worker via Mix Task

### Task 4: Create the `Mix.Tasks.Mr.SpawnWorker` Mix task

**Files:**
- Create: `apps/mr_master/lib/mix/tasks/mr/spawn_worker.ex`

- [ ] **Step 1: Create the file with argument parsing**

Create `apps/mr_master/lib/mix/tasks/mr/spawn_worker.ex` with this content:

```elixir
defmodule Mix.Tasks.Mr.SpawnWorker do
  require Logger
  use Mix.Task

  @impl Mix.Task
  def run(args) do
    defaults = [
      master: "master@127.0.0.1"
    ]

    {parsed, _argv, _errors} =
      OptionParser.parse(args,
        strict: [master: :string, coords: :string]
      )

    options = Keyword.merge(defaults, parsed)

    {:ok, master_node_str} = Keyword.fetch(options, :master)
    master_node = String.to_atom(master_node_str)

    # Parse or generate coordinates
    coords = parse_coords(Keyword.get(options, :coords, nil))

    # Generate unique worker node name using timestamp
    timestamp = System.monotonic_time(:millisecond)
    worker_name = "worker_#{timestamp}@127.0.0.1"

    Logger.info("[spawner] Spawning worker: #{worker_name} with coords #{elem(coords, 0)}, #{elem(coords, 1)}")

    # Validate master is reachable (best-effort)
    case :net_kernel.connect_node(master_node) do
      true ->
        Logger.debug("[spawner] Master node #{master_node} is reachable")

      false ->
        Logger.warning("[spawner] Could not reach master node #{master_node}, proceeding anyway")

      :ignored ->
        Logger.warning("[spawner] Could not connect to master node #{master_node}, proceeding anyway")
    end

    # Spawn the worker process
    env_vars = "MR_START_MASTER=false MR_COORDS=#{elem(coords, 0)},#{elem(coords, 1)}"
    command = "sh -c '#{env_vars} elixir --name #{worker_name} --cookie secret -S mix run --no-halt'"

    port = Port.open({:spawn, command}, [])

    # Brief wait for worker to start and register
    Process.sleep(500)

    # Check if master now knows about this worker (best-effort)
    case :rpc.call(master_node, MrMaster.Master, :get_workers, [], 1_000) do
      {:badrpc, _reason} ->
        Logger.warning("[spawner] Could not verify worker registration (master unreachable)")

      workers ->
        worker_node = String.to_atom(worker_name)

        case Map.has_key?(workers, worker_node) do
          true ->
            Logger.info("[spawner] Worker #{worker_name} registered successfully")
            IO.puts(worker_name)

          false ->
            Logger.warning(
              "[spawer] Worker spawned but not yet registered. It may register shortly."
            )

            IO.puts(worker_name)
        end
    end
  end

  # Parse coordinates from string "x,y" or return random coords
  defp parse_coords(nil) do
    {Enum.random(0..99) * 1.0, Enum.random(0..99) * 1.0}
  end

  defp parse_coords(coords_str) do
    case String.split(coords_str, ",") do
      [x_str, y_str] ->
        case {Float.parse(x_str), Float.parse(y_str)} do
          {{x, ""}, {y, ""}} ->
            # Clamp to [0, 100]
            x = max(0.0, min(100.0, x))
            y = max(0.0, min(100.0, y))
            {x, y}

          _ ->
            Logger.error("[spawner] Could not parse coords '#{coords_str}', using random")
            {Enum.random(0..99) * 1.0, Enum.random(0..99) * 1.0}
        end

      _ ->
        Logger.error("[spawner] Coords must be in format 'x,y', got '#{coords_str}'")
        {Enum.random(0..99) * 1.0, Enum.random(0..99) * 1.0}
    end
  end
end
```

- [ ] **Step 2: Commit**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
git add apps/mr_master/lib/mix/tasks/mr/spawn_worker.ex
git commit -m "feat: create Mix.Tasks.Mr.SpawnWorker to spawn workers on demand"
```

### Task 5: Test the Mix task manually

**Files:**
- No files created/modified (testing only)

- [ ] **Step 1: Start master with initial workers**

In one terminal:

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
mix mr.start --task word_count --workers 2 --reducers 2
```

The master should start with 2 workers.

- [ ] **Step 2: In another terminal, spawn a new worker**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
mix mr.spawn_worker --master master@127.0.0.1
```

Expected output:
```
[spawner] Spawning worker: worker_1716936000123@127.0.0.1 with coords 45.0, 67.0
[spawer] Worker spawned but not yet registered. It may register shortly.
worker_1716936000123@127.0.0.1
```

Wait 2 seconds and check the master logs — you should see the new worker registered.

- [ ] **Step 3: Spawn a worker with specific coordinates**

```bash
mix mr.spawn_worker --master master@127.0.0.1 --coords 10,20
```

Expected: Worker spawns with coords 10.0, 20.0

- [ ] **Step 4: Verify worker appears on dashboard**

Open `http://localhost:4000` and verify:
- New worker appears on the 2D grid within ~1 second
- Worker can be selected and killed via the dashboard
- Worker is assigned tasks (if job is still running)

- [ ] **Step 5: Note for committer**

No commit needed for this task — it's manual verification only.

---

### Task 6: Add "Add Worker" button to dashboard header

**Files:**
- Modify: `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex`

- [ ] **Step 1: Locate the worker header section**

Find this section in `dashboard_live.html.heex` (around line 46-52):

```heex
<h2 class="text-sm font-semibold uppercase tracking-widest text-gray-400">
  Worker Nodes
  <span class="ml-1 text-gray-500 normal-case tracking-normal font-normal">
    (<%= map_size(@workers) %> registered)
  </span>
</h2>
```

Replace it with:

```heex
<div class="flex items-center justify-between">
  <h2 class="text-sm font-semibold uppercase tracking-widest text-gray-400">
    Worker Nodes
    <span class="ml-1 text-gray-500 normal-case tracking-normal font-normal">
      (<%= map_size(@workers) %> registered)
    </span>
  </h2>
  <button
    class="px-3 py-1 bg-green-600 hover:bg-green-700 text-white text-xs font-semibold rounded transition-colors"
    phx-click="spawn_worker"
    title="Add a new worker node"
  >
    + Add Worker
  </button>
</div>
```

- [ ] **Step 2: Commit**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
git add apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex
git commit -m "feat: add 'Add Worker' button to dashboard header"
```

### Task 7: Add event handler for spawn_worker button

**Files:**
- Modify: `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex`

- [ ] **Step 1: Add `:spawn_error` to socket assigns in mount**

Update the `mount/3` function to initialize the `:spawn_error` assign:

```elixir
@impl true
def mount(_params, _session, socket) do
  socket = assign(socket, 
    selected_worker: nil, 
    confirm_kill: nil, 
    final_elapsed_time: nil, 
    task_type: nil,
    spawn_error: nil
  )

  if connected?(socket) do
    Process.send_after(self(), :update_state, @poll_interval_ms)
  end

  {:ok, assign_master_state(socket, fetch_master_state())}
end
```

- [ ] **Step 2: Add spawn_error to nil and state assign cases**

In `assign_master_state(socket, nil)`:

```elixir
defp assign_master_state(socket, nil) do
  assign(socket,
    master_connected: false,
    workers: %{},
    map_tasks: %{},
    reduce_tasks: %{},
    phase: :waiting,
    recent_events: [],
    worker_memory: %{},
    elapsed_time: nil,
    job_complete: false,
    final_elapsed_time: nil,
    task_type: nil,
    spawn_error: nil
  )
end
```

And in `assign_master_state(socket, state)`, add `spawn_error: socket.assigns[:spawn_error]` to the assign call.

- [ ] **Step 3: Add handle_event for spawn_worker button**

Add this new event handler to `dashboard_live.ex`, after the existing `handle_event` clauses (around line 60):

```elixir
@impl true
def handle_event("spawn_worker", _params, socket) do
  # Spawn a new worker by calling the Mix task
  case System.cmd("mix", ["mr.spawn_worker", "--master", "master@127.0.0.1"], stderr_to_stdout: true) do
    {output, 0} ->
      # Success — worker name is in output
      worker_name = String.trim(output)
      Logger.info("[dashboard] Worker spawned: #{worker_name}")
      {:noreply, assign(socket, spawn_error: nil)}

    {error_output, exit_code} ->
      # Failure — show error to user
      error_msg = "Failed to spawn worker (exit code #{exit_code})"
      Logger.warning("[dashboard] #{error_msg}: #{error_output}")
      {:noreply, assign(socket, spawn_error: error_msg)}
  end
end
```

- [ ] **Step 4: Commit**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
git add apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex
git commit -m "feat: add spawn_worker event handler to dashboard"
```

### Task 8: Display error messages from worker spawn

**Files:**
- Modify: `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex`

- [ ] **Step 1: Add error toast at the top of the template**

Add this near the very top of `dashboard_live.html.heex`, right after the opening `<div class="h-screen...">` tag (before the header):

```heex
<%= if @spawn_error do %>
  <div class="fixed top-4 right-4 px-4 py-3 bg-red-600 text-white rounded shadow-lg text-sm">
    <%= @spawn_error %>
  </div>
<% end %>
```

This displays a red toast notification in the top-right if there's a spawn error.

- [ ] **Step 2: Commit**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
git add apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex
git commit -m "feat: show error toast when worker spawn fails"
```

### Task 9: End-to-end test of worker spawning

**Files:**
- No files created/modified (testing only)

- [ ] **Step 1: Start master with initial workers**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
mix mr.start --task word_count --workers 2 --reducers 2
```

- [ ] **Step 2: Open dashboard and verify initial state**

Navigate to `http://localhost:4000`:
- Should show 2 workers on the 2D grid
- "Add Worker" button should be visible in the worker header
- Task type should show "Word Count"

- [ ] **Step 3: Click "Add Worker" button multiple times**

Click the button 3 times and verify:
- Each click adds a new worker to the grid within ~1 second
- All workers should be visible and responsive
- Dashboard remains responsive

- [ ] **Step 4: Verify workers are functional**

- Select workers and verify they show memory usage
- Kill a spawned worker via the dashboard and verify it disappears
- Verify spawned workers receive task assignments (if job is still running)

- [ ] **Step 5: Test error case (optional)**

To test error handling, try spawning with an invalid master:

```bash
# From another terminal
mix mr.spawn_worker --master invalid@127.0.0.1
```

The dashboard should show an error toast if you try to spawn with an unreachable master.

- [ ] **Step 6: Note for committer**

No commit needed for this task — it's end-to-end verification only.

---

## Final Verification & Cleanup

### Task 10: Verify all tests pass

**Files:**
- No files created/modified (testing only)

- [ ] **Step 1: Run existing dashboard tests**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce/apps/mr_dashboard
mix test
```

Expected: All tests pass (or at least no new failures from our changes).

- [ ] **Step 2: Run all project tests**

```bash
cd /Users/dash/Code/github/babys-first-mapreduce
mix test
```

Expected: All tests pass.

- [ ] **Step 3: Note for committer**

If any tests fail, investigate and fix before moving to next task.

---

## Spec Coverage Checklist

- [x] Feature 1: Display task type in header — Task 1-3
- [x] Feature 1: Extract task_module from master state — Task 1
- [x] Feature 1: Format module name to human-readable — Task 1
- [x] Feature 1: Handle missing/unknown task type — Task 1 (format_task_name handles nil and unknown)
- [x] Feature 2: Create Mix task to spawn workers — Task 4
- [x] Feature 2: Accept --master and --coords args — Task 4
- [x] Feature 2: Generate unique node names — Task 4 (uses timestamp)
- [x] Feature 2: Validate master node reachability — Task 4
- [x] Feature 2: Clamp coordinates to [0, 100] — Task 4
- [x] Feature 2: Add "Add Worker" button — Task 6
- [x] Feature 2: Invoke Mix task from dashboard — Task 7
- [x] Feature 2: Show error toast on failure — Task 8
- [x] Edge case: Master unreachable — Task 4 (graceful degradation), Task 8 (error display)
- [x] Edge case: Node name collision — Task 4 (timestamp prevents)
- [x] Edge case: Rapid button clicks — Task 7 (no debounce, but ok for dev)
- [x] Testing: Manual verification of both features — Task 3, 5, 9
