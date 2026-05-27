defmodule MrDashboardWeb.DashboardLive do
  use MrDashboardWeb, :live_view

  @poll_interval_ms 100

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Process.send_after(self(), :update_state, @poll_interval_ms)
    end

    {:ok, assign_master_state(socket, fetch_master_state())}
  end

  @impl true
  def handle_info(:update_state, socket) do
    Process.send_after(self(), :update_state, @poll_interval_ms)
    {:noreply, assign_master_state(socket, fetch_master_state())}
  end

  # --- Data fetching ---

  defp fetch_master_state do
    try do
      GenServer.call(MrMaster.Master, :get_state, 1_000)
    catch
      :exit, _ -> nil
    end
  end

  defp assign_master_state(socket, nil) do
    assign(socket,
      master_connected: false,
      workers: %{},
      map_tasks: %{},
      reduce_tasks: %{},
      phase: :waiting,
      recent_events: []
    )
  end

  defp assign_master_state(socket, state) do
    assign(socket,
      master_connected: true,
      workers: state.workers,
      map_tasks: state.map_tasks,
      reduce_tasks: state.reduce_tasks,
      phase: state.phase,
      recent_events: Map.get(state, :recent_events, [])
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

  # Worker status colours — use actual statuses from MrProtocol.WorkerInfo:
  # :idle | :busy | :dead
  defp status_color(:idle), do: "#22c55e"
  defp status_color(:busy), do: "#3b82f6"
  defp status_color(:dead), do: "#ef4444"
  defp status_color(_), do: "#6b7280"

  # coords are {x, y} floats in 0.0–1.0 range; convert to CSS percentages
  defp coords_pct({x, y}), do: {Float.round(x * 100, 2), Float.round(y * 100, 2)}
end
