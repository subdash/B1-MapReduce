# LiveView Dashboard Plan

**Date:** 2026-05-27  
**Goal:** Build a real-time web dashboard showing worker status, task progress, and interactive controls for fault injection.  
**Approach:** Two-phase implementation. Phase 1 is read-only visualization of system state. Phase 2 adds interactive controls.

---

## Architecture Overview

The dashboard will be a separate Phoenix app in the umbrella (`mr_dashboard`) that:
1. Starts alongside the master and workers
2. Polls the master's state every 100ms via `GenServer.call`
3. Serves a LiveView that updates in real-time
4. Allows users to kill workers or set throttles (Phase 2)

**Why separate app:** Keeps the MapReduce core independent of web UI. Easy to disable the dashboard in production if needed.

---

## Phase 1: Read-Only Dashboard

### Task 1.1: Add Phoenix to umbrella and create mr_dashboard app

**What to build:** Scaffold a new Phoenix app in the umbrella with LiveView dependencies.

**Files to create/modify:**
- Modify `mix.exs` — add `mr_dashboard` to umbrella apps
- Create `apps/mr_dashboard/mix.exs` with Phoenix, LiveView, and Tailwind deps
- Create `apps/mr_dashboard/lib/mr_dashboard/application.ex` with Phoenix Endpoint
- Create `apps/mr_dashboard/config/config.exs` with endpoint config
- Modify `config/dev.exs` to add dashboard endpoint config

**Details:**

`mix new apps/mr_dashboard --live` (if available in Elixir version) or manual setup:

```elixir
# apps/mr_dashboard/mix.exs dependencies:
{:phoenix, "~> 1.7.0"},
{:phoenix_liveview, "~> 0.20.0"},
{:tailwind, "~> 0.2.0", runtime: Mix.env() == :dev},
{:esbuild, "~> 0.8.0", runtime: Mix.env() == :dev}
```

Phoenix Endpoint in `mr_dashboard/lib/mr_dashboard_web/endpoint.ex`:
```elixir
defmodule MrDashboardWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :mr_dashboard
  
  @session_options [
    store: :cookie,
    key: "_mr_dashboard_key",
    signing_salt: "random_salt"
  ]

  socket "/live", Phoenix.LiveView.Socket, websocket: [connect_info: [session: @session_options]]

  # ... other endpoint config
end
```

**Why:** Phoenix provides the HTTP server and LiveView gives us real-time updates without page reloads.

---

### Task 1.2: Build LiveView component for worker grid visualization

**What to build:** A 100×100 grid showing all workers as colored squares, positioned by their (x, y) coordinates. Color indicates status: green (idle), blue (in-progress), red (dead).

**Files to create:**
- Create `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex` — main LiveView module
- Create `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex` — template

**Details:**

```elixir
# dashboard_live.ex
defmodule MrDashboardWeb.DashboardLive do
  use MrDashboardWeb, :live_view
  require Logger

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(100, self(), :update_state)
    end
    
    {:ok, fetch_state(socket)}
  end

  @impl true
  def handle_info(:update_state, socket) do
    {:noreply, fetch_state(socket)}
  end

  defp fetch_state(socket) do
    state = GenServer.call(MrMaster.Master, :get_state)
    
    assign(socket,
      master_state: state,
      workers: state.workers,
      map_tasks: state.map_tasks,
      reduce_tasks: state.reduce_tasks,
      phase: state.phase,
      job_progress: calculate_progress(state)
    )
  end

  defp calculate_progress(state) do
    total_map = map_size(state.map_tasks)
    completed_map = Enum.count(state.map_tasks, fn {_, task} -> task.status == :completed end)
    
    total_reduce = map_size(state.reduce_tasks)
    completed_reduce = Enum.count(state.reduce_tasks, fn {_, task} -> task.status == :completed end)
    
    %{
      map_percent: if(total_map > 0, do: round(completed_map / total_map * 100), else: 0),
      reduce_percent: if(total_reduce > 0, do: round(completed_reduce / total_reduce * 100), else: 0)
    }
  end
end
```

Template snippet:
```heex
<!-- dashboard_live.html.heex -->
<div class="p-8 bg-gray-900 text-white h-screen">
  <h1 class="text-3xl mb-6">MapReduce Dashboard</h1>
  
  <!-- Worker Grid -->
  <div class="mb-8">
    <h2 class="text-xl mb-4">Worker Nodes (100×100 grid)</h2>
    <div class="relative w-96 h-96 border border-gray-600 bg-gray-800">
      <%= for {node, worker} <- @workers do %>
        <div 
          class="absolute w-4 h-4 rounded-full cursor-pointer"
          style={"left: #{worker.coords |> elem(0)}%; top: #{worker.coords |> elem(1)}%; background-color: #{status_color(worker.status)}"}
          title={"#{node}: #{worker.status}"}
        >
        </div>
      <% end %>
    </div>
    <div class="mt-4 text-sm">
      <span class="inline-block w-3 h-3 bg-green-500 mr-2"></span>Idle
      <span class="inline-block w-3 h-3 bg-blue-500 mr-2 ml-4"></span>In Progress
      <span class="inline-block w-3 h-3 bg-red-500 mr-2 ml-4"></span>Dead
    </div>
  </div>
  
  <!-- Progress Bars -->
  <div class="mb-8">
    <h2 class="text-xl mb-4">Job Progress</h2>
    <div class="mb-4">
      <p class="text-sm mb-2">Map Phase: <%= @job_progress.map_percent %>%</p>
      <div class="w-96 h-4 bg-gray-700 rounded">
        <div class="h-4 bg-green-500 rounded" style={"width: #{@job_progress.map_percent}%"}></div>
      </div>
    </div>
    <div class="mb-4">
      <p class="text-sm mb-2">Reduce Phase: <%= @job_progress.reduce_percent %>%</p>
      <div class="w-96 h-4 bg-gray-700 rounded">
        <div class="h-4 bg-blue-500 rounded" style={"width: #{@job_progress.reduce_percent}%"}></div>
      </div>
    </div>
  </div>

  <!-- Event Log -->
  <div>
    <h2 class="text-xl mb-4">Recent Events</h2>
    <div class="bg-gray-800 p-4 rounded h-48 overflow-y-auto font-mono text-sm">
      <!-- Populated by Phase 1.3 -->
    </div>
  </div>
</div>
```

Helper function:
```elixir
defp status_color(:idle), do: "#22c55e"   # green
defp status_color(:in_progress), do: "#3b82f6"  # blue
defp status_color(:dead), do: "#ef4444"  # red
```

**Why:** Gives a real-time visual representation of the system. Users can see at a glance which workers are active and how far the job has progressed.

---

### Task 1.3: Add event log display

**What to build:** A scrolling log showing recent events from the master's logger.

**Files to modify:**
- `apps/mr_master/lib/mr_master/master.ex` — add `handle_call(:get_recent_events, ...)` 
- `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex` — fetch and display events

**Details:**

Master maintains a fixed-size in-memory buffer of recent log events (last 50):

```elixir
# In MrMaster.Master state struct:
defstruct [..., recent_events: []]

# Add to handle_call:
@impl true
def handle_call(:get_recent_events, _from, state) do
  {:reply, state.recent_events, state}
end

# Helper to log events and add to buffer:
defp log_event(state, event_string) do
  Logger.info(event_string)
  new_events = [event_string | state.recent_events] |> Enum.take(50)
  Map.put(state, :recent_events, new_events)
end
```

Update existing log calls to use `log_event`:
```elixir
# Old:
Logger.info("[master] task_assigned | type=map id=3 worker=worker2@localhost")

# New:
state = log_event(state, "[master] task_assigned | type=map id=3 worker=worker2@localhost")
```

LiveView displays the events:
```heex
<div class="bg-gray-800 p-4 rounded h-48 overflow-y-auto font-mono text-sm">
  <%= for event <- Enum.reverse(@master_state.recent_events) do %>
    <div class="text-gray-300 mb-1"><%= event %></div>
  <% end %>
</div>
```

**Why:** Gives users visibility into what's happening in real-time without needing to read raw logs.

---

### Task 1.4: Add worker detail sidebar

**What to build:** When a user hovers over a worker square, show a sidebar with that worker's stats.

**Files to modify:**
- `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex`
- `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex`

**Details:**

LiveView state tracks selected worker:
```elixir
@impl true
def mount(_params, _session, socket) do
  # ...
  {:ok, assign(socket, selected_worker: nil)}
end

def handle_event("select_worker", %{"node" => node_name}, socket) do
  {:noreply, assign(socket, selected_worker: String.to_atom(node_name))}
end
```

Template:
```heex
<div class="flex gap-8">
  <!-- Grid on left -->
  <div>
    <div class="relative w-96 h-96 border border-gray-600 bg-gray-800">
      <%= for {node, worker} <- @workers do %>
        <div 
          class={"absolute w-4 h-4 rounded-full cursor-pointer " <> if node == @selected_worker, do: "ring-2 ring-white", else: ""}
          phx-click="select_worker"
          phx-value-node={Atom.to_string(node)}
          style={"left: #{worker.coords |> elem(0)}%; top: #{worker.coords |> elem(1)}%; background-color: #{status_color(worker.status)}"}
        >
        </div>
      <% end %>
    </div>
  </div>

  <!-- Sidebar on right -->
  <%= if @selected_worker && Map.has_key?(@workers, @selected_worker) do %>
    <% worker = @workers[@selected_worker] %>
    <div class="bg-gray-800 p-4 rounded w-64">
      <h3 class="text-lg font-bold mb-4"><%= @selected_worker %></h3>
      <div class="space-y-2 text-sm">
        <p>Status: <span class="font-mono"><%= worker.status %></span></p>
        <p>Coords: (<%= elem(worker.coords, 0) |> round %>, <%= elem(worker.coords, 1) |> round %>)</p>
        <%= if worker.status == :in_progress do %>
          <p>Current Task: task-<%= worker.current_task_id || "none" %></p>
        <% end %>
        <!-- Phase 2: Kill/throttle buttons here -->
      </div>
    </div>
  <% end %>
</div>
```

**Why:** Gives detailed information about each worker without cluttering the main view.

---

### Task 1.5: Add task details table

**What to build:** A table showing all map and reduce tasks, their status, which worker is running them, and how long they've been running.

**Files to create:**
- Add a new tab/section to `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex`

**Details:**

Template (map tasks table):
```heex
<div class="mt-8">
  <h2 class="text-xl mb-4">Map Tasks</h2>
  <div class="overflow-x-auto">
    <table class="w-full text-sm border-collapse">
      <thead>
        <tr class="bg-gray-700">
          <th class="border border-gray-600 p-2 text-left">Task ID</th>
          <th class="border border-gray-600 p-2 text-left">Status</th>
          <th class="border border-gray-600 p-2 text-left">Worker</th>
          <th class="border border-gray-600 p-2 text-left">Duration (ms)</th>
        </tr>
      </thead>
      <tbody>
        <%= for {id, task} <- Enum.sort_by(@map_tasks, fn {id, _} -> id end) do %>
          <tr class="border-b border-gray-700 hover:bg-gray-700">
            <td class="border border-gray-600 p-2"><%= id %></td>
            <td class="border border-gray-600 p-2"><%= task.status %></td>
            <td class="border border-gray-600 p-2"><%= task.assigned_to || "-" %></td>
            <td class="border border-gray-600 p-2"><%= task.duration_ms || "-" %></td>
          </tr>
        <% end %>
      </tbody>
    </table>
  </div>
</div>
```

**Why:** Users can drill down into individual tasks to see progress and identify stragglers.

---

## Phase 2: Interactive Controls

### Task 2.1: Add kill_worker and set_throttle messages to Master

**What to build:** New `handle_call` messages in the master that allow external processes to kill a worker or set its latency.

**Files to modify:**
- `apps/mr_master/lib/mr_master/master.ex`

**Details:**

```elixir
@impl true
def handle_call({:kill_worker, node}, _from, state) do
  Logger.info("[master] kill_worker requested | node=#{node}")
  Node.disconnect(node)  # Force disconnect
  # The :nodedown message will be handled naturally
  {:reply, :ok, state}
end

@impl true
def handle_call({:set_throttle, node, multiplier}, _from, state) do
  Logger.info("[master] set_throttle | node=#{node} multiplier=#{multiplier}")
  GenServer.cast({MrWorker.Worker, node}, {:set_throttle, multiplier})
  {:reply, :ok, state}
end
```

**Why:** Provides the underlying mechanism for interactive controls. No LiveView knowledge needed in the master.

---

### Task 2.2: Add kill and throttle buttons to LiveView

**What to build:** Add buttons to the worker detail sidebar that trigger kill and throttle actions.

**Files to modify:**
- `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex` (add event handlers)
- `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex` (add buttons)

**Details:**

Event handlers in LiveView:
```elixir
def handle_event("kill_worker", %{"node" => node_name}, socket) do
  node = String.to_atom(node_name)
  GenServer.call(MrMaster.Master, {:kill_worker, node})
  {:noreply, assign(socket, selected_worker: nil)}  # Clear selection after kill
end

def handle_event("set_throttle", %{"node" => node_name, "multiplier" => multiplier_str}, socket) do
  node = String.to_atom(node_name)
  multiplier = String.to_float(multiplier_str)
  GenServer.call(MrMaster.Master, {:set_throttle, node, multiplier})
  {:noreply, socket}
end
```

Template buttons:
```heex
<%= if @selected_worker && Map.has_key?(@workers, @selected_worker) do %>
  <% worker = @workers[@selected_worker] %>
  <div class="bg-gray-800 p-4 rounded w-64">
    <!-- ... existing worker info ... -->
    
    <div class="mt-6 space-y-2">
      <button 
        phx-click="kill_worker"
        phx-value-node={Atom.to_string(@selected_worker)}
        class="w-full bg-red-600 hover:bg-red-700 text-white p-2 rounded text-sm"
      >
        Kill Worker
      </button>
      
      <div>
        <label class="text-xs block mb-1">Throttle Multiplier:</label>
        <div class="flex gap-2">
          <input 
            type="number"
            id="throttle_input"
            min="0.1"
            step="0.1"
            value="1.0"
            class="w-16 px-2 py-1 rounded text-black"
          />
          <button
            phx-click="set_throttle"
            phx-value-node={Atom.to_string(@selected_worker)}
            phx-value-multiplier={document.getElementById("throttle_input").value}
            class="flex-1 bg-orange-600 hover:bg-orange-700 text-white p-2 rounded text-sm"
          >
            Apply
          </button>
        </div>
      </div>
    </div>
  </div>
<% end %>
```

**Why:** Turns the dashboard into an active tool for fault injection and testing, not just observation.

---

### Task 2.3: Add confirmation dialog for destructive actions

**What to build:** A modal that confirms before killing a worker (to prevent accidental clicks).

**Files to modify:**
- `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.ex`
- `apps/mr_dashboard/lib/mr_dashboard_web/live/dashboard_live.html.heex`

**Details:**

LiveView state tracks confirmation dialog:
```elixir
@impl true
def mount(_params, _session, socket) do
  # ...
  {:ok, assign(socket, confirm_kill: nil)}  # nil or node name
end

def handle_event("request_kill", %{"node" => node_name}, socket) do
  {:noreply, assign(socket, confirm_kill: String.to_atom(node_name))}
end

def handle_event("confirm_kill", %{"node" => node_name}, socket) do
  node = String.to_atom(node_name)
  GenServer.call(MrMaster.Master, {:kill_worker, node})
  {:noreply, assign(socket, confirm_kill: nil, selected_worker: nil)}
end

def handle_event("cancel_kill", _, socket) do
  {:noreply, assign(socket, confirm_kill: nil)}
end
```

Modal template:
```heex
<%= if @confirm_kill do %>
  <div class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center">
    <div class="bg-gray-800 p-6 rounded border border-red-500 max-w-sm">
      <h3 class="text-lg font-bold mb-4">Confirm Kill</h3>
      <p class="mb-6">Are you sure you want to kill <code><%= @confirm_kill %></code>?</p>
      <div class="flex gap-4">
        <button
          phx-click="cancel_kill"
          class="flex-1 bg-gray-600 hover:bg-gray-700 text-white p-2 rounded"
        >
          Cancel
        </button>
        <button
          phx-click="confirm_kill"
          phx-value-node={Atom.to_string(@confirm_kill)}
          class="flex-1 bg-red-600 hover:bg-red-700 text-white p-2 rounded"
        >
          Kill
        </button>
      </div>
    </div>
  </div>
<% end %>
```

**Why:** Prevents accidental fault injection and keeps the system stable during testing.

---

## Phase 3: Enhancements (Optional)

### Task 3.1: Historical metrics and graphs

Store task duration data and plot them as graphs (response time trends, etc.).

### Task 3.2: Job replay

Save the event log and allow replay of a completed job to visualize what happened.

### Task 3.3: Dark/light theme toggle

Add a simple theme switcher.

---

## Review Checkpoints

After each task:
1. **Task 1.1:** App scaffolds successfully, `mix ecto.setup` and `mix phx.server` work.
2. **Task 1.2:** Grid renders with correct colors. Hovering shows worker info.
3. **Task 1.3:** Events are logged to master state and appear in the log display.
4. **Task 1.4:** Clicking a worker shows its sidebar with correct details.
5. **Task 1.5:** Tasks table populates correctly and updates every 100ms.
6. **Task 2.1:** Calling `:kill_worker` and `:set_throttle` on the master succeeds.
7. **Task 2.2:** Buttons appear in the sidebar and trigger the correct LiveView events.
8. **Task 2.3:** Killing a worker requires confirmation; canceling leaves the worker alive.

---

## Integration with Multi-Machine

The dashboard works seamlessly with the multi-machine setup:
- Master runs on one machine with the dashboard
- Workers on same or different machines connect to master
- Dashboard polls master's state like any other client
- No code changes needed; just ensure the master's HTTP port is accessible

---

## Implementation Order

**Phase 1 (Core, ~3-4 hours):**
1. Task 1.1 — Create Phoenix app
2. Task 1.2 — Worker grid visualization
3. Task 1.3 — Event log
4. Task 1.4 — Worker sidebar
5. Task 1.5 — Task table

**Phase 2 (Interactive, ~2 hours):**
1. Task 2.1 — Add master handlers
2. Task 2.2 — Add LiveView buttons
3. Task 2.3 — Confirmation dialog

**Phase 3 (Polish, optional):**
1. Task 3.1 — Historical metrics
2. Task 3.2 — Job replay
3. Task 3.3 — Theme toggle

---

## Non-Goals

- Real-time log streaming (polling is simpler for a prototype)
- Network topology visualization (defer to phase 3 if needed)
- Multi-job management (single job focus)
- Authentication (not needed for local testing)
