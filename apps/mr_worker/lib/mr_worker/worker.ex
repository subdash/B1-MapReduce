defmodule MrWorker.Worker do
  # Entrypoint for a worker node.
  require Logger
  use GenServer

  def init(opts) do
    master_node = Keyword.fetch!(opts, :master_node)
    coords = Keyword.fetch!(opts, :coords)

    # Don't connect/register during init -- instead, schedule it async
    Process.send_after(self(), :connect_to_master, 100)

    # Return initial state
    {:ok,
     %{coords: coords, master_node: master_node, throttle_multiplier: 1.0, connection_attempt: 0}}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def handle_cast({:run_map, task, task_module, _registry}, state) do
    master_node = state.master_node

    Task.start(fn ->
      try do
        {:ok, locations} = MrWorker.MapTask.execute(task, task_module, node())
        GenServer.cast({MrMaster.Master, master_node}, {:map_done, task.id, locations})
      rescue
        e ->
          Logger.error("[worker] map_task crashed | id=#{task.id} error=#{inspect(e)}")
      end
    end)

    Logger.info("[worker] map_started | id=#{task.id}")
    {:noreply, state}
  end

  def handle_cast({:run_reduce, task, task_module, registry}, state) do
    master_node = state.master_node
    multiplier = state.throttle_multiplier

    Task.start(fn ->
      try do
        :ok = MrWorker.ReduceTask.execute(task, task_module, registry, "output/", multiplier)
        GenServer.cast({MrMaster.Master, master_node}, {:reduce_done, task.id})
      rescue
        e ->
          Logger.error("[worker] reduce_task crashed | id=#{task.id} error=#{inspect(e)}")
      end
    end)

    Logger.info("[worker] reduce_started | id=#{task.id}")
    {:noreply, state}
  end

  def handle_cast({:set_throttle, multiplier}, state) do
    {:noreply, %{state | throttle_multiplier: multiplier}}
  end

  def handle_info(:connect_to_master, state) do
    master_node = state.master_node

    case try_connect(master_node, state) do
      :ok ->
        Logger.info("[worker] connected to master")
        {:noreply, state}

      :retry ->
        # Exponential backoff capped at 5 seconds
        backoff_ms = min(100 * Integer.pow(2, state.connection_attempt), 5000)

        Logger.debug(
          "[worker] connection attempt #{state.connection_attempt + 1}, retrying in #{backoff_ms}ms"
        )

        Process.send_after(self(), :connect_to_master, backoff_ms)
        {:noreply, %{state | connection_attempt: state.connection_attempt + 1}}
    end
  end

  defp try_connect(master_node, state) do
    max_attempts = 100
    attempt = state.connection_attempt

    if attempt >= max_attempts do
      raise "Failed to connect to master after #{attempt} attempts"
    end

    case Node.connect(master_node) do
      true ->
        # Try to register
        case GenServer.call(
               {MrMaster.Master, master_node},
               {:register, %MrProtocol.WorkerInfo{node: node(), coords: state.coords}},
               5000
             ) do
          :ok -> :ok
          _ -> :retry
        end

      false ->
        :retry
    end
  rescue
    _ -> :retry
  end
end
