defmodule MrMaster.Master do
  use GenServer

  defstruct workers: %{},
            map_tasks: %{},
            reduce_tasks: %{},
            intermediate_locations: %{},
            task_module: nil,
            num_reducers: 0,
            phase: :waiting

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :net_kernel.monitor_nodes(true)
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:register, worker_info}, _from, state) do
    worker_node = worker_info.node
    # Add the new worker node to the registry
    updated_state = put_in(state.workers[worker_node], worker_info)
    # Assign pending tasks since we now have a new worker
    final_state = assign_pending_tasks(updated_state)
    {:reply, :ok, final_state}
  end

  @impl true
  def handle_call({:submit_job, opts}, _from, state) do
    map_tasks = MrMaster.Job.create_map_tasks(opts.input_dir, opts.num_reducers)
    map_tasks_hashmap = Map.new(map_tasks, fn task -> {task.id, task} end)

    state_updates = %{
      :map_tasks => map_tasks_hashmap,
      :phase => :mapping,
      :num_reducers => opts.num_reducers,
      :task_module => opts.task_module
    }

    updated_state = Map.merge(state, state_updates)
    # Assign pending tasks since we now have a new MapReduce job 
    final_state = assign_pending_tasks(updated_state)
    {:reply, :ok, final_state}
  end

  @impl true
  def handle_cast(_opts, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(_status, state) do
    {:noreply, state}
  end

  defp assign_pending_tasks(%{phase: :mapping} = state) do
    # Find an unassigned map task
    with %MrProtocol.MapTask{} = idle_map_task <-
           Enum.find(
             Map.values(state.map_tasks),
             nil,
             fn %MrProtocol.MapTask{} = map_task ->
               map_task.status == :idle
             end
           ),
         # Find a node with an unassigned worker
         idle_worker_node <- MrMaster.Scheduler.assign_map_task(state.workers),
         # Find the corresponding worker to the node
         %MrProtocol.WorkerInfo{} = idle_worker <- state.workers[idle_worker_node] do
      # Assign the task to the worker node 
      task_assigned = %MrProtocol.MapTask{
        idle_map_task
        | :assigned_to => idle_worker_node,
          :status => :in_progress
      }

      # Update the worker as busy
      worker_assigned = %MrProtocol.WorkerInfo{
        idle_worker
        | :status => :busy
      }

      # Update state
      updated_state = put_in(state.map_tasks[task_assigned.id], task_assigned)

      updated_state = put_in(updated_state.workers[idle_worker_node], worker_assigned)

      # Recursively call the function until there are no more idle tasks or workers
      assign_pending_tasks(updated_state)
    else
      nil -> state
    end
  end

  defp assign_pending_tasks(%{phase: :reducing} = state) do
    # Find an unassigned reduce task
    with %MrProtocol.ReduceTask{} = idle_reduce_task <-
           Enum.find(Map.values(state.reduce_tasks), nil, fn %MrProtocol.ReduceTask{} =
                                                               reduce_task ->
             reduce_task.status == :idle
           end),
         reduce_worker_nodes <-
           Enum.map(idle_reduce_task.locations, fn {node, _file_path} -> node end),
         idle_worker_node <-
           MrMaster.Scheduler.assign_reduce_task(state.workers, reduce_worker_nodes),
         %MrProtocol.WorkerInfo{} = idle_worker <- state.workers[idle_worker_node] do
      # Assign the task to the worker node 
      task_assigned = %MrProtocol.ReduceTask{
        idle_reduce_task
        | :assigned_to => idle_worker_node,
          :status => :in_progress
      }

      # Update the worker as busy
      worker_assigned = %MrProtocol.WorkerInfo{
        idle_worker
        | :status => :busy
      }

      # Update state
      updated_state = put_in(state.reduce_tasks[task_assigned.id], task_assigned)

      updated_state = put_in(updated_state.workers[idle_worker_node], worker_assigned)

      # Recursively call the function until there are no more idle tasks or workers
      assign_pending_tasks(updated_state)
    else
      nil -> state
    end
  end

  defp assign_pending_tasks(state) do
    # catch-all for :waiting or :done — just return unchanged
    state
  end
end
