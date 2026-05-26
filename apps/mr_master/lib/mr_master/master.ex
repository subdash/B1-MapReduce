defmodule MrMaster.Master do
  use GenServer
  require Logger

  defstruct workers: %{},
            map_tasks: %{},
            reduce_tasks: %{},
            backup_tasks: %{},
            intermediate_locations: %{},
            task_module: nil,
            num_reducers: 0,
            phase: :waiting,
            start_time: nil,
            input_dir: nil

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :net_kernel.monitor_nodes(true)
    Process.send_after(self(), :check_stragglers, 10_000)
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
      :task_module => opts.task_module,
      :start_time => System.monotonic_time(:millisecond),
      :input_dir => opts.input_dir
    }

    updated_state = Map.merge(state, state_updates)

    # Log job started
    task_name =
      try do
        opts.task_module |> Module.split() |> List.last()
      rescue
        ArgumentError ->
          if is_nil(opts.task_module), do: "unknown", else: Atom.to_string(opts.task_module)
      end

    Logger.info(
      "[master] job_started | task=#{task_name} files=#{map_size(map_tasks_hashmap)} reducers=#{opts.num_reducers} workers=#{map_size(state.workers)}"
    )

    # Assign pending tasks since we now have a new MapReduce job
    final_state = assign_pending_tasks(updated_state)
    {:reply, :ok, final_state}
  end

  @impl true
  def handle_cast({:map_done, task_id, bucket_locations}, state) do
    # If primary task is completed already, that means a bakcup map task
    # just sent its :map_done message. We must mark the backup task as completed.
    if state.map_tasks[task_id].status == :completed do
      state =
        case Map.fetch(state.backup_tasks, {:map, task_id}) do
          {:ok, backup_node} ->
            # Find worker assigned to backup task
            %MrProtocol.WorkerInfo{} = backup_worker = state.workers[backup_node]

            # Mark worker as idle in worker registry
            state =
              put_in(state.workers[backup_node], %MrProtocol.WorkerInfo{
                backup_worker
                | status: :idle
              })

            # Remove the task from backup_tasks
            state = Map.update!(state, :backup_tasks, &Map.delete(&1, {:map, task_id}))
            assign_pending_tasks(state)

          :error ->
            state
        end

      {:noreply, state}
    else
      # The primary (rather than backup) map task completed.
      # Mark the task as completed in the map tasks dictionary.
      %MrProtocol.MapTask{} = completed_task = state.map_tasks[task_id]
      completed_task = %MrProtocol.MapTask{completed_task | status: :completed}
      # Mark worker as idle
      %MrProtocol.WorkerInfo{} = idle_worker = state.workers[completed_task.assigned_to]

      idle_worker = %MrProtocol.WorkerInfo{
        idle_worker
        | status: :idle
      }

      # Update state with completed task, idle worker and bucket locations
      state = put_in(state.map_tasks[task_id], completed_task)
      state = put_in(state.intermediate_locations[task_id], bucket_locations)
      state = put_in(state.workers[completed_task.assigned_to], idle_worker)
      # Assign any pending map tasks
      state = assign_pending_tasks(state)
      state = maybe_start_reduce(state)

      {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:reduce_done, task_id}, state) do
    if state.reduce_tasks[task_id].status == :completed do
      state =
        case Map.fetch(state.backup_tasks, {:reduce, task_id}) do
          {:ok, backup_node} ->
            %MrProtocol.WorkerInfo{} = backup_worker = state.workers[backup_node]

            state =
              put_in(state.workers[backup_node], %MrProtocol.WorkerInfo{
                backup_worker
                | status: :idle
              })

            state = Map.update!(state, :backup_tasks, &Map.delete(&1, {:reduce, task_id}))
            assign_pending_tasks(state)

          :error ->
            state
        end

      {:noreply, state}
    else
      %MrProtocol.ReduceTask{} = task = state.reduce_tasks[task_id]
      %MrProtocol.WorkerInfo{} = worker = state.workers[task.assigned_to]
      # Mark task as completed
      state =
        put_in(state.reduce_tasks[task_id], %MrProtocol.ReduceTask{task | status: :completed})

      state = put_in(state.workers[worker.node], %MrProtocol.WorkerInfo{worker | status: :idle})
      state = assign_pending_tasks(state)

      reduce_phase_complete =
        Enum.all?(Map.values(state.reduce_tasks), fn %MrProtocol.ReduceTask{} = reduce_task ->
          reduce_task.status == :completed
        end)

      if reduce_phase_complete do
        state = %{state | phase: :done}
        duration = System.monotonic_time(:millisecond) - state.start_time

        Logger.info(
          "[master] job_complete | duration_ms=#{duration} map_tasks=#{map_size(state.map_tasks)} reduce_tasks=#{map_size(state.reduce_tasks)}"
        )

        {:noreply, state}
      else
        {:noreply, state}
      end
    end
  end

  @impl true
  def handle_cast({:set_throttle, node, multiplier}, state) do
    %MrProtocol.WorkerInfo{} = worker = state.workers[node]

    state =
      put_in(state.workers[node], %MrProtocol.WorkerInfo{worker | throttle_multiplier: multiplier})

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    %MrProtocol.WorkerInfo{} = worker = state.workers[node]
    dead_worker = %MrProtocol.WorkerInfo{worker | status: :dead}
    state = put_in(state.workers[node], dead_worker)

    # Update all map tasks assigned to the node which are in progress or completed,
    # as well as any reduce tasks in progress assigned to the node. Set them as idle
    # and remove the assignee so that they can be retried.
    updated_map_tasks =
      Map.new(state.map_tasks, fn {task_id, task} ->
        %MrProtocol.MapTask{} = task

        if task.assigned_to == node and task.status in [:in_progress, :completed] do
          {task_id, %MrProtocol.MapTask{task | status: :idle, assigned_to: nil}}
        else
          {task_id, task}
        end
      end)

    num_updated_map_tasks =
      Enum.count(state.map_tasks, fn {_task_id, task} ->
        task.assigned_to == node and task.status in [:in_progress, :completed]
      end)

    updated_reduce_tasks =
      Map.new(state.reduce_tasks, fn {task_id, task} ->
        %MrProtocol.ReduceTask{} = task

        if task.assigned_to == node and task.status == :in_progress do
          {task_id, %MrProtocol.ReduceTask{task | status: :idle, assigned_to: nil}}
        else
          {task_id, task}
        end
      end)

    num_updated_reduce_tasks =
      Enum.count(state.reduce_tasks, fn {_task_id, task} ->
        task.assigned_to == node and task.status == :in_progress
      end)

    state = put_in(state.map_tasks, updated_map_tasks)
    state = put_in(state.reduce_tasks, updated_reduce_tasks)
    # Reassign tasks
    state = assign_pending_tasks(state)
    tasks_updated = num_updated_map_tasks + num_updated_reduce_tasks
    Logger.info("[master] node_down | node=#{node} requeued=#{tasks_updated}")

    {:noreply, state}
  end

  @impl true
  def handle_info(:check_stragglers, %{phase: :mapping} = state) do
    # Reschedule timer
    Process.send_after(self(), :check_stragglers, 10_000)
    total_tasks = map_size(state.map_tasks)
    completed_tasks = Enum.count(state.map_tasks, fn {_, t} -> t.status == :completed end)

    if total_tasks > 0 and completed_tasks / total_tasks > 0.8 do
      {:noreply, assign_backup_tasks(state, :map)}
    else
      {:noreply, state}
    end
  end

  def handle_info(:check_stragglers, %{phase: :reducing} = state) do
    # Reschedule timer
    Process.send_after(self(), :check_stragglers, 10_000)
    total_tasks = map_size(state.reduce_tasks)
    completed_tasks = Enum.count(state.reduce_tasks, fn {_, t} -> t.status == :completed end)

    if total_tasks > 0 and completed_tasks / total_tasks > 0.8 do
      {:noreply, assign_backup_tasks(state, :reduce)}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:check_stragglers, %{phase: :done} = state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(:check_stragglers, state) do
    Process.send_after(self(), :check_stragglers, 10_000)
    {:noreply, state}
  end

  @impl true
  def handle_info(_status, state) do
    {:noreply, state}
  end

  defp assign_backup_tasks(state, :map = type) do
    with {node, %MrProtocol.WorkerInfo{} = worker_info} <-
           Enum.find(state.workers, nil, fn {_node, worker} -> worker.status == :idle end),
         {task_id, task} <-
           Enum.find(state.map_tasks, nil, fn {task_id, task} ->
             task.status == :in_progress and
               not Map.has_key?(state.backup_tasks, {type, task_id})
           end) do
      state = put_in(state.backup_tasks[{type, task_id}], node)
      state = put_in(state.workers[node], %MrProtocol.WorkerInfo{worker_info | status: :busy})

      Logger.info(
        "[master] backup_launched | task=map:#{task_id} primary=#{task.assigned_to} backup=#{node}"
      )

      assign_backup_tasks(state, type)
    else
      _ -> state
    end
  end

  defp assign_backup_tasks(state, :reduce = type) do
    with {node, %MrProtocol.WorkerInfo{} = worker_info} <-
           Enum.find(state.workers, nil, fn {_node, worker} -> worker.status == :idle end),
         {task_id, task} <-
           Enum.find(state.reduce_tasks, nil, fn {task_id, task} ->
             task.status == :in_progress and
               not Map.has_key?(state.backup_tasks, {type, task_id})
           end) do
      state = put_in(state.backup_tasks[{type, task_id}], node)
      state = put_in(state.workers[node], %MrProtocol.WorkerInfo{worker_info | status: :busy})

      Logger.info(
        "[master] backup_launched | task=reduce:#{task_id} primary=#{task.assigned_to} backup=#{node}"
      )

      assign_backup_tasks(state, type)
    else
      _ -> state
    end
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
        | assigned_to: idle_worker_node,
          status: :in_progress
      }

      # Update the worker as busy
      worker_assigned = %MrProtocol.WorkerInfo{
        idle_worker
        | status: :busy
      }

      # Log task assignment
      Logger.info(
        "[master] task_assigned | type=map id=#{task_assigned.id} worker=#{idle_worker_node} dist=N/A"
      )

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
        | assigned_to: idle_worker_node,
          status: :in_progress
      }

      # Update the worker as busy
      worker_assigned = %MrProtocol.WorkerInfo{
        idle_worker
        | status: :busy
      }

      # Calculate mean distance to all map worker nodes
      distances =
        Enum.map(reduce_worker_nodes, fn map_node ->
          map_worker_coords = state.workers[map_node].coords
          MrProtocol.Distance.euclidean_distance(idle_worker.coords, map_worker_coords)
        end)

      mean_distance = Enum.sum(distances) / length(distances)

      # Log task assignment
      Logger.info(
        "[master] task_assigned | type=reduce id=#{task_assigned.id} worker=#{idle_worker_node} dist=#{Float.round(mean_distance, 1)}"
      )

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

  defp maybe_start_reduce(state) do
    map_task_is_complete = fn %MrProtocol.MapTask{} = map_task ->
      map_task.status == :completed
    end

    # Only transition to reduce phase if all map tasks are complete.
    if Enum.all?(Map.values(state.map_tasks), map_task_is_complete) do
      # Otherwise we can move on to the reducing phase of the MapReduce task.
      # First we iterate over all bucket indexes
      reduce_tasks =
        for bucket <- 0..(state.num_reducers - 1) do
          # Get the intermediate file locations -- the output from the map tasks
          locations =
            for {task_id, task} <- state.map_tasks do
              case Map.fetch(state.intermediate_locations, task_id) do
                {:ok, bucket_map} ->
                  {task.assigned_to, bucket_map[bucket]}

                :error ->
                  raise "BUG: map task #{task_id} is completed but has no intermediate locations"
              end
            end

          # Construct a reduce task -- we can reuse the bucket index as the task id
          # and bucket id
          %MrProtocol.ReduceTask{
            id: bucket,
            bucket: bucket,
            locations: locations
          }
        end

      # Convert the list to a mapping of task id to the task definition itself
      reduce_tasks_map = Map.new(reduce_tasks, fn task -> {task.id, task} end)
      # Load the tasks into the state and set the state to :reducing
      updated_state = %{state | reduce_tasks: reduce_tasks_map, phase: :reducing}
      # Assign the reduce tasks
      assign_pending_tasks(updated_state)
    else
      state
    end
  end
end
