defmodule MrDashboardWeb.DashboardLive do
  use MrDashboardWeb, :live_view

  @poll_interval_ms 100

  @impl true
  def mount(_params, _session, socket) do
    # Initialise UI-only state before the first data fetch so assign_master_state
    # never has to worry about these keys not existing.
    socket = assign(socket, selected_worker: nil, confirm_kill: nil)

    if connected?(socket) do
      Process.send_after(self(), :update_state, @poll_interval_ms)
    end

    {:ok, assign_master_state(socket, fetch_master_state())}
  end

  # --- Polling ---

  @impl true
  def handle_info(:update_state, socket) do
    Process.send_after(self(), :update_state, @poll_interval_ms)
    {:noreply, assign_master_state(socket, fetch_master_state())}
  end

  # --- UI events ---

  @impl true
  def handle_event("select_worker", %{"node" => node_str}, socket) do
    node = String.to_atom(node_str)
    # Toggle: clicking the selected worker deselects it
    selected = if socket.assigns.selected_worker == node, do: nil, else: node
    {:noreply, assign(socket, selected_worker: selected, confirm_kill: nil)}
  end

  @impl true
  def handle_event("request_kill", %{"node" => node_str}, socket) do
    {:noreply, assign(socket, confirm_kill: String.to_atom(node_str))}
  end

  @impl true
  def handle_event("cancel_kill", _params, socket) do
    {:noreply, assign(socket, confirm_kill: nil)}
  end

  @impl true
  def handle_event("confirm_kill", %{"node" => node_str}, socket) do
    node = String.to_atom(node_str)

    try do
      GenServer.call(MrMaster.Master, {:kill_worker, node}, 2_000)
    catch
      :exit, _ -> :ok
    end

    {:noreply, assign(socket, selected_worker: nil, confirm_kill: nil)}
  end

  # --- Data fetching ---

  defp fetch_master_state do
    try do
      GenServer.call(MrMaster.Master, :get_state, 1_000)
    catch
      :exit, _ -> nil
    end
  end

  # Fetch total memory from each live worker node concurrently via RPC.
  # Returns %{node => bytes | nil}.
  defp fetch_worker_memory(workers) do
    workers
    |> Enum.filter(fn {_, w} -> w.status != :dead end)
    |> Task.async_stream(
      fn {node, _} ->
        mem =
          case :rpc.call(node, :erlang, :memory, [:total], 300) do
            {:badrpc, _} -> nil
            bytes -> bytes
          end

        {node, mem}
      end,
      max_concurrency: 20,
      timeout: 500,
      on_timeout: :kill_task
    )
    |> Enum.reduce(%{}, fn
      {:ok, {node, mem}}, acc -> Map.put(acc, node, mem)
      _error, acc -> acc
    end)
  end

  defp assign_master_state(socket, nil) do
    assign(socket,
      master_connected: false,
      workers: %{},
      map_tasks: %{},
      reduce_tasks: %{},
      phase: :waiting,
      recent_events: [],
      worker_memory: %{}
    )
  end

  defp assign_master_state(socket, state) do
    assign(socket,
      master_connected: true,
      workers: state.workers,
      map_tasks: state.map_tasks,
      reduce_tasks: state.reduce_tasks,
      phase: state.phase,
      recent_events: Map.get(state, :recent_events, []),
      worker_memory: fetch_worker_memory(state.workers)
    )
  end

  # --- View helpers ---

  defp task_progress(tasks) do
    total = map_size(tasks)
    completed = Enum.count(tasks, fn {_, t} -> t.status == :completed end)
    in_progress = Enum.count(tasks, fn {_, t} -> t.status == :in_progress end)

    %{
      total: total,
      completed: completed,
      in_progress: in_progress,
      percent: if(total > 0, do: round(completed / total * 100), else: 0)
    }
  end

  # Statuses from MrProtocol.WorkerInfo: :idle | :busy | :dead
  defp status_color(:idle), do: "#22c55e"
  defp status_color(:busy), do: "#3b82f6"
  defp status_color(:dead), do: "#ef4444"
  defp status_color(_), do: "#6b7280"

  defp status_label(:idle), do: "idle"
  defp status_label(:busy), do: "busy"
  defp status_label(:dead), do: "dead"
  defp status_label(s), do: to_string(s)

  # Coords from mr.start are Enum.random(0..99) * 1.0 — already 0–99, which maps
  # directly to CSS percentages in a 100×100 grid. Do NOT multiply by 100.
  defp coords_pct({x, y}), do: {Float.round(x, 1), Float.round(y, 1)}

  defp format_memory(nil), do: "—"

  defp format_memory(bytes) when bytes >= 1_048_576,
    do: "#{Float.round(bytes / 1_048_576, 1)} MB"

  defp format_memory(bytes) when bytes >= 1_024,
    do: "#{Float.round(bytes / 1_024, 1)} KB"

  defp format_memory(bytes), do: "#{bytes} B"

  # Returns the task ID string currently assigned to this node, or nil.
  defp current_task(node, map_tasks, reduce_tasks) do
    map_hit =
      Enum.find_value(map_tasks, fn {id, t} ->
        if t.assigned_to == node and t.status == :in_progress, do: "map:#{id}"
      end)

    reduce_hit =
      Enum.find_value(reduce_tasks, fn {id, t} ->
        if t.assigned_to == node and t.status == :in_progress, do: "reduce:#{id}"
      end)

    map_hit || reduce_hit
  end
end
