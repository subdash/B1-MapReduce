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
            input_dir: nil,
            recent_events: []

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
  def handle_call(:get_workers, _from, state) do
    {:reply, state.workers, state}
  end

  @impl true
  def handle_call(:get_phase, _from, state) do
    {:reply, state.phase, state}
  end

  @impl true
  def handle_call(:ready, _from, state) do
    ready = state.phase in [:waiting, :mapping, :reducing, :done]
    {:reply, if(ready, do: :ok, else: :retry), state}
  end

  @impl true
  def handle_call({:register, worker_info}, _from, state) do
    worker_node = worker_info.node
    # Add the new worker node to the registry
    state = put_in(state.workers[worker_node], worker_info)

    state =
      log_event(
        state,
        "[master] worker_registered | node=#{worker_node} coords=#{inspect(worker_info.coords)}"
      )

    # Assign pending tasks since we now have a new worker
    state = assign_pending_tasks(state)

    {:reply, :ok, state}
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

    updated_state =
      log_event(
        updated_state,
        "[master] job_started | task=#{task_name} files=#{map_size(map_tasks_hashmap)} reducers=#{opts.num_reducers} workers=#{map_size(state.workers)}"
      )

    # Assign pending tasks since we now have a new MapReduce job
    final_state = assign_pending_tasks(updated_state)
    {:reply, :ok, final_state}
  end

  @impl true
  def handle_call({:kill_worker, node}, _from, state) do
    state = log_event(state, "[master] kill_worker | node=#{node}")
    # Send a graceful OTP shutdown — worker deregisters from epmd itself.
    # The subsequent {:nodedown, node} message handles task re-queuing.
    :rpc.cast(node, :init, :stop, [])
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:map_done, task_id, sender_node, bucket_locations}, state) do
    cond do
      # Drop completions that aren't from the task's current owner (see sender_owns_task?/4).
      # The worker's intermediate output is unreachable and the task is already being re-run.
      not sender_owns_task?(state, :map, task_id, sender_node) ->
        Logger.debug("[master] stale map_done ignored | id=#{task_id} from=#{sender_node}")
        {:noreply, state}

      # The primary already finished this task, so this is its backup straggler reporting in.
      state.map_tasks[task_id].status == :completed ->
        {:noreply, complete_backup_task(state, :map, task_id)}

      # Normal case: the assigned primary worker finished the task.
      true ->
        {:noreply, complete_primary_map_task(state, task_id, bucket_locations)}
    end
  end

  @impl true
  def handle_cast({:reduce_done, task_id, sender_node}, state) do
    cond do
      # Drop completions that aren't from the task's current owner (see sender_owns_task?/4).
      not sender_owns_task?(state, :reduce, task_id, sender_node) ->
        Logger.debug("[master] stale reduce_done ignored | id=#{task_id} from=#{sender_node}")
        {:noreply, state}

      # The primary already finished this task, so this is its backup straggler reporting in.
      state.reduce_tasks[task_id].status == :completed ->
        {:noreply, complete_backup_task(state, :reduce, task_id)}

      # Normal case: the assigned primary worker finished the task.
      true ->
        {:noreply, complete_primary_reduce_task(state, task_id)}
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

    # If the job is already done, worker deaths are expected (graceful shutdown
    # or dashboard kill). Don't reset task assignments — that would show incorrect
    # 0% map progress on the dashboard after a completed job.
    if state.phase == :done do
      state = log_event(state, "[master] node_down | node=#{node} (job complete, no requeue)")
      {:noreply, state}
    else
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
        Map.new(state.reduce_tasks, fn {task_id, task = %MrProtocol.ReduceTask{}} ->
          has_stale_locations =
            Enum.any?(task.locations, fn {map_worker_node, _file_path} ->
              map_worker_node == node
            end)

          if task.status == :in_progress and (task.assigned_to == node or has_stale_locations) do
            {task_id,
             %MrProtocol.ReduceTask{task | status: :idle, assigned_to: nil, locations: []}}
          else
            {task_id, task}
          end
        end)

      num_updated_reduce_tasks =
        Enum.count(state.reduce_tasks, fn {_task_id, task} ->
          Enum.any?(task.locations, fn {map_worker_node, _file_path} ->
            map_worker_node == node
          end)
        end)

      map_task_ids_to_requeue =
        state.map_tasks
        |> Enum.filter(fn {_id, task} ->
          task.assigned_to == node and task.status in [:in_progress, :completed]
        end)
        |> Enum.map(fn {id, _task} -> id end)

      state = put_in(state.map_tasks, updated_map_tasks)
      state = put_in(state.reduce_tasks, updated_reduce_tasks)
      state = assign_pending_tasks(state)
      tasks_updated = num_updated_map_tasks + num_updated_reduce_tasks
      state = log_event(state, "[master] node_down | node=#{node} requeued=#{tasks_updated}")

      state =
        if state.phase == :reducing do
          updated_intermediate_locations =
            Map.drop(state.intermediate_locations, map_task_ids_to_requeue)

          state = %{
            state
            | phase: :mapping,
              intermediate_locations: updated_intermediate_locations
          }

          log_event(
            state,
            "[master] phase_rolled_back_to_mapping | node=#{node} reason=map_worker_died_during_reduce"
          )
        else
          state
        end

      {:noreply, state}
    end
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

  # A task completion is only legitimate from the worker that *currently* owns the task —
  # its assigned primary, or the backup launched for it as a straggler. This guards against
  # a completion still in flight from a worker that has since died: node_down either requeues
  # the task (clearing assigned_to) or reassigns it, so the sender no longer matches the owner.
  # Such a message must be dropped — the on-disk output it refers to is on an unreachable node,
  # and the task is already being re-run — otherwise we'd record a dead location and crash on
  # the requeued (assigned_to: nil) case.
  defp sender_owns_task?(state, :map, task_id, sender_node) do
    state.map_tasks[task_id].assigned_to == sender_node or
      state.backup_tasks[{:map, task_id}] == sender_node
  end

  defp sender_owns_task?(state, :reduce, task_id, sender_node) do
    state.reduce_tasks[task_id].assigned_to == sender_node or
      state.backup_tasks[{:reduce, task_id}] == sender_node
  end

  # The primary already completed; the backup straggler has now reported in. Free the backup
  # worker and forget the backup bookkeeping. Identical for map and reduce, hence the `type`.
  defp complete_backup_task(state, type, task_id) do
    case Map.fetch(state.backup_tasks, {type, task_id}) do
      {:ok, backup_node} ->
        state = mark_worker_idle(state, backup_node)
        state = Map.update!(state, :backup_tasks, &Map.delete(&1, {type, task_id}))
        assign_pending_tasks(state)

      :error ->
        state
    end
  end

  defp complete_primary_map_task(state, task_id, bucket_locations) do
    %MrProtocol.MapTask{} = task = state.map_tasks[task_id]
    completed_task = %MrProtocol.MapTask{task | status: :completed}

    state = put_in(state.map_tasks[task_id], completed_task)
    state = put_in(state.intermediate_locations[task_id], bucket_locations)
    state = mark_worker_idle(state, completed_task.assigned_to)
    state = assign_pending_tasks(state)
    maybe_start_reduce(state)
  end

  defp complete_primary_reduce_task(state, task_id) do
    %MrProtocol.ReduceTask{} = task = state.reduce_tasks[task_id]
    completed_task = %MrProtocol.ReduceTask{task | status: :completed}

    state = put_in(state.reduce_tasks[task_id], completed_task)
    state = mark_worker_idle(state, completed_task.assigned_to)
    state = assign_pending_tasks(state)

    if reduce_phase_complete?(state), do: finish_job(state), else: state
  end

  defp reduce_phase_complete?(state) do
    Enum.all?(Map.values(state.reduce_tasks), fn %MrProtocol.ReduceTask{} = task ->
      task.status == :completed
    end)
  end

  defp finish_job(state) do
    state = %{state | phase: :done}
    duration = System.monotonic_time(:millisecond) - state.start_time

    log_event(
      state,
      "[master] job_complete | duration_ms=#{duration} map_tasks=#{map_size(state.map_tasks)} reduce_tasks=#{map_size(state.reduce_tasks)}"
    )
  end

  defp mark_worker_idle(state, node) do
    %MrProtocol.WorkerInfo{} = worker = state.workers[node]
    put_in(state.workers[node], %MrProtocol.WorkerInfo{worker | status: :idle})
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

      state =
        log_event(
          state,
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

      state =
        log_event(
          state,
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

      # Update state
      updated_state = put_in(state.map_tasks[task_assigned.id], task_assigned)
      updated_state = put_in(updated_state.workers[idle_worker_node], worker_assigned)

      # Log task assignment
      updated_state =
        log_event(
          updated_state,
          "[master] task_assigned | type=map id=#{task_assigned.id} worker=#{idle_worker_node} dist=N/A"
        )

      # Send the task to the worker
      GenServer.cast(
        {MrWorker.Worker, idle_worker_node},
        {:run_map, task_assigned, state.task_module, state.workers}
      )

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

      # Update state
      updated_state = put_in(state.reduce_tasks[task_assigned.id], task_assigned)
      updated_state = put_in(updated_state.workers[idle_worker_node], worker_assigned)

      # Log task assignment
      updated_state =
        log_event(
          updated_state,
          "[master] task_assigned | type=reduce id=#{task_assigned.id} worker=#{idle_worker_node} dist=#{Float.round(mean_distance, 1)}"
        )

      # Send the task to the worker
      GenServer.cast(
        {MrWorker.Worker, idle_worker_node},
        {:run_reduce, task_assigned, state.task_module, state.workers}
      )

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

  # Logs a message via Logger and appends it to the in-memory recent_events
  # buffer (capped at 50 entries) so the LiveView dashboard can display it.
  defp log_event(state, message) do
    Logger.info(message)
    recent = [message | state.recent_events] |> Enum.take(50)
    %{state | recent_events: recent}
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
            |> Enum.filter(fn {_node, location} -> not is_nil(location) end)

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
