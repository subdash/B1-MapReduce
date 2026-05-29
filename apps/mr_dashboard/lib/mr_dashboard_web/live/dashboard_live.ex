defmodule MrDashboardWeb.DashboardLive do
  use MrDashboardWeb, :live_view

  @poll_interval_ms 100

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

  @impl true
  def handle_event("throttle_worker", %{"node" => node_str}, socket) do
    node = String.to_atom(node_str)
    GenServer.cast(MrMaster.Master, {:set_throttle, node, 0.5})
    {:noreply, socket}
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
      worker_memory: %{},
      elapsed_time: nil,
      job_complete: false,
      final_elapsed_time: nil,
      task_type: nil
    )
  end

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

  defp format_elapsed_ms(nil), do: nil

  defp format_elapsed_ms(start_time) do
    elapsed_ms = System.monotonic_time(:millisecond) - start_time
    seconds = div(elapsed_ms, 1000)
    millis = rem(elapsed_ms, 1000)
    {seconds, millis}
  end

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
    |> String.replace(~r/([a-z])([A-Z])|([A-Z])([A-Z][a-z])/, "\\1\\3 \\2\\4")
  end
end
