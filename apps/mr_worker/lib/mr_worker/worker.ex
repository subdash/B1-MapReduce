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
        {file_name, file_contents} =
          MrWorker.ReduceTask.execute(task, task_module, registry, multiplier)

        MrWorker.RPC.call(
          master_node,
          MrMaster.OutputCollector,
          {:write_output, file_name, file_contents},
          registry,
          multiplier
        )

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
    max_attempts = 100

    if state.connection_attempt >= max_attempts do
      Logger.error(
        "[worker] giving up: master unreachable after #{max_attempts} attempts | " <>
          "node=#{state.master_node}"
      )

      System.stop(1)
      {:noreply, state}
    else
      case try_connect(state.master_node, state.coords) do
        :ok ->
          Logger.info("[worker] registered with master | node=#{state.master_node}")
          {:noreply, state}

        :retry ->
          # Exponential backoff capped at 5 seconds
          backoff_ms = min(100 * Integer.pow(2, state.connection_attempt), 5000)

          Logger.debug(
            "[worker] register attempt #{state.connection_attempt + 1} failed, " <>
              "retry in #{backoff_ms}ms"
          )

          Process.send_after(self(), :connect_to_master, backoff_ms)
          {:noreply, %{state | connection_attempt: state.connection_attempt + 1}}
      end
    end
  end

  # Returns :ok | :retry and raises only for the non-retryable error
  defp try_connect(master_node, coords) do
    case Node.connect(master_node) do
      true ->
        register(master_node, coords)

      false ->
        # Master not yet reachable -- worth retrying
        :retry

      :ignored ->
        raise "[worker] cannot connect: distribution not started (net_kernel.start did not run)"
    end
  end

  defp register(master_node, coords) do
    worker_info = %MrProtocol.WorkerInfo{node: node(), coords: coords}

    try do
      case GenServer.call(
             {MrMaster.Master, master_node},
             {:register, worker_info},
             5000
           ) do
        :ok ->
          :ok

        other ->
          Logger.warning("[worker] unexpected register reply | reply=#{inspect(other)}")
          :retry
      end
    catch
      # GenServer.call signals failure by exiting, not raising. Timeout, :noproc, :nodedown 
      # are all handled here and would not be handled by rescue. We do not want to rescue the
      # error class.
      :exit, reason ->
        Logger.debug("[worker] register call failed | reason=#{inspect(reason)}")
        :retry
    end
  end
end
