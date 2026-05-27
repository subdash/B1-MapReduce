defmodule MrWorker.Worker do
  # Entrypoint for a worker node.
  require Logger
  use GenServer

  def init(opts) do
    master_node = Keyword.fetch!(opts, :master_node)
    coords = Keyword.fetch!(opts, :coords)
    # Connect to master
    Node.connect(master_node)

    # Register with master
    GenServer.call(
      {MrMaster.Master, master_node},
      {:register, %MrProtocol.WorkerInfo{node: node(), coords: coords}}
    )

    # Return initial state
    {:ok, %{coords: coords, master_node: master_node, throttle_multiplier: 1.0}}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def handle_cast({:run_map, task, task_module, _registry}, state) do
    master_node = state.master_node

    Task.start(fn ->
      {:ok, locations} = MrWorker.MapTask.execute(task, task_module, node())
      GenServer.cast({MrMaster.Master, master_node}, {:map_done, task.id, locations})
    end)

    Logger.info("[worker] map_started | id=#{task.id}")
    {:noreply, state}
  end

  def handle_cast({:run_reduce, task, task_module, registry}, state) do
    master_node = state.master_node
    multiplier = state.throttle_multiplier

    Task.start(fn ->
      :ok = MrWorker.ReduceTask.execute(task, task_module, registry, "output/", multiplier)
      GenServer.cast({MrMaster.Master, master_node}, {:reduce_done, task.id})
    end)

    Logger.info("[worker] reduce_started | id=#{task.id}")
    {:noreply, state}
  end

  def handle_cast({:set_throttle, multiplier}, state) do
    {:noreply, %{state | throttle_multiplier: multiplier}}
  end
end
