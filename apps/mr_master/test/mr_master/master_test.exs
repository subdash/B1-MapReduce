defmodule MasterTest do
  use ExUnit.Case, async: false

  setup do
    start_supervised!(MrMaster.Master)
    :ok
  end

  test "a registered worker appears in the worker list" do
    w = worker_info(:worker1@localhost)
    GenServer.call({MrMaster.Master, node()}, {:register, w})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    registered_worker =
      Enum.find(state.workers, fn {node, _worker} -> node == :worker1@localhost end)

    assert registered_worker != nil
  end

  @tag :tmp_dir
  test "submitting a job creates tasks and assigns them to idle workers", %{tmp_dir: tmp_dir} do
    create_files(tmp_dir, 10)
    w = worker_info(:worker1@localhost)

    GenServer.call({MrMaster.Master, node()}, {:register, w})
    GenServer.call({MrMaster.Master, node()}, {:submit_job, job_opts(tmp_dir)})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.workers[w.node].status == :busy
    assert map_size(state.map_tasks) == 10
  end

  @tag :tmp_dir
  test "reduce tasks are created with the correct locations after map completion", %{
    tmp_dir: tmp_dir
  } do
    create_files(tmp_dir, 1)
    w = worker_info(:worker1@localhost)

    GenServer.call({MrMaster.Master, node()}, {:register, w})
    GenServer.call({MrMaster.Master, node()}, {:submit_job, job_opts(tmp_dir)})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.phase == :mapping

    [{task_id, task}] = Map.to_list(state.map_tasks)
    worker_node = task.assigned_to
    locs = bucket_locations(worker_node, state.num_reducers)

    GenServer.cast({MrMaster.Master, node()}, {:map_done, task_id, worker_node, locs})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.map_tasks[task_id].status == :completed

    assert state.phase == :reducing
    assert map_size(state.reduce_tasks) == 1
    [{0, reduce_task}] = Map.to_list(state.reduce_tasks)
    assert reduce_task.bucket == 0
    assert {worker_node, locs[0]} in reduce_task.locations
  end

  @tag :tmp_dir
  test "phase transitions to done after a full MapReduce job", %{
    tmp_dir: tmp_dir
  } do
    create_files(tmp_dir, 1)
    w = worker_info(:worker1@localhost)

    GenServer.call({MrMaster.Master, node()}, {:register, w})

    GenServer.call(
      {MrMaster.Master, node()},
      {:submit_job, job_opts(tmp_dir, task_module: :fake_task2)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    [{task_id, task}] = Map.to_list(state.map_tasks)

    GenServer.cast(
      {MrMaster.Master, node()},
      {:map_done, task_id, task.assigned_to, bucket_locations(task.assigned_to, state.num_reducers)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.map_tasks[task_id].status == :completed
    [{reduce_id, reduce_task}] = Map.to_list(state.reduce_tasks)
    GenServer.cast({MrMaster.Master, node()}, {:reduce_done, reduce_id, reduce_task.assigned_to})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.phase == :done
  end

  @tag :tmp_dir
  test "phase remains reducing while some tasks are still in progress", %{tmp_dir: tmp_dir} do
    create_files(tmp_dir, 1)
    w = worker_info(:worker1@localhost)

    GenServer.call({MrMaster.Master, node()}, {:register, w})

    GenServer.call(
      {MrMaster.Master, node()},
      {:submit_job, job_opts(tmp_dir, num_reducers: 2, task_module: :fake_task3)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    [{task_id, task}] = Map.to_list(state.map_tasks)

    GenServer.cast(
      {MrMaster.Master, node()},
      {:map_done, task_id, task.assigned_to, bucket_locations(task.assigned_to, state.num_reducers)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.map_tasks[task_id].status == :completed
    assert state.phase == :reducing
    assert map_size(state.reduce_tasks) == 2

    {reduce_id, reduce_task} = Enum.find(state.reduce_tasks, fn {_id, t} -> t.status == :in_progress end)
    GenServer.cast({MrMaster.Master, node()}, {:reduce_done, reduce_id, reduce_task.assigned_to})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    assert state.phase == :reducing
  end

  @tag :tmp_dir
  test "tasks are re-queued correctly when a nodedown message is received", %{tmp_dir: tmp_dir} do
    create_files(tmp_dir, 2)
    w1 = worker_info(:worker1@localhost)
    w2 = worker_info(:worker2@localhost)

    GenServer.call({MrMaster.Master, node()}, {:register, w1})
    GenServer.call({MrMaster.Master, node()}, {:register, w2})

    GenServer.call(
      {MrMaster.Master, node()},
      {:submit_job, job_opts(tmp_dir, task_module: :fake_task3)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    {_, w1_task} =
      Enum.find(state.map_tasks, fn {_id, t} -> t.assigned_to == :worker1@localhost end)

    {_, w2_task} =
      Enum.find(state.map_tasks, fn {_id, t} -> t.assigned_to == :worker2@localhost end)

    # Complete map task for worker 1
    GenServer.cast(
      {MrMaster.Master, node()},
      {:map_done, w1_task.id, w1_task.assigned_to, bucket_locations(w1_task.assigned_to, state.num_reducers)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.map_tasks[w1_task.id].status == :completed

    # Kill node 2
    Process.send(GenServer.whereis(MrMaster.Master), {:nodedown, w2.node}, [])

    # Assert that worker 2 is dead and that its task was reassigned to worker 1
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.workers[w2.node].status == :dead
    assert state.map_tasks[w2_task.id].assigned_to == w1.node
    assert state.map_tasks[w2_task.id].status == :in_progress

    # Kill node 1. Assert that it is dead and that its task was not reassigned
    # due to all 2/2 workers being dead.
    Process.send(GenServer.whereis(MrMaster.Master), {:nodedown, w1.node}, [])
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.workers[w1.node].status == :dead
    assert state.map_tasks[w2_task.id].status == :idle
  end

  @tag :tmp_dir
  test "backup tasks are assigned when there are stragglers", %{tmp_dir: tmp_dir} do
    # Setup workers and job
    create_files(tmp_dir, 6)
    w1 = worker_info(:worker1@localhost)
    w2 = worker_info(:worker2@localhost)

    GenServer.call({MrMaster.Master, node()}, {:register, w1})
    GenServer.call({MrMaster.Master, node()}, {:register, w2})

    GenServer.call(
      {MrMaster.Master, node()},
      {:submit_job, job_opts(tmp_dir, task_module: :fake_task3, num_reducers: 6)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    # Find map task assigned to worker 1
    {w1_task_id, _w1_task} =
      Enum.find(state.map_tasks, fn {_id, t} -> t.assigned_to == :worker1@localhost end)

    for _ <- 0..4 do
      state = GenServer.call({MrMaster.Master, node()}, :get_state)

      # Complete every task assigned to worker 2
      {w2_task_id, _w2_task} =
        Enum.find(state.map_tasks, fn {_id, t} ->
          t.assigned_to == :worker2@localhost and t.status == :in_progress
        end)

      locs = bucket_locations(w2.node, state.num_reducers)
      GenServer.cast({MrMaster.Master, node()}, {:map_done, w2_task_id, w2.node, locs})
    end

    # At this point, >80% of map tasks have been completed, so we assign a backup
    # task that worker 2 can take on, the same task worker 1 is on.
    Process.send(GenServer.whereis(MrMaster.Master), :check_stragglers, [])
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.map_tasks[w1_task_id].status == :in_progress
    assert Map.has_key?(state.backup_tasks, {:map, w1_task_id})
    assert state.backup_tasks[{:map, w1_task_id}] == w2.node
    # Mark the task as complete
    locs = bucket_locations(w1.node, state.num_reducers)
    GenServer.cast({MrMaster.Master, node()}, {:map_done, w1_task_id, w1.node, locs})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    locs = bucket_locations(w2.node, state.num_reducers)
    GenServer.cast({MrMaster.Master, node()}, {:map_done, w1_task_id, w2.node, locs})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.phase == :reducing
    # Find reduce task assigned to worker 1
    {w1_task_id, _w1_task} =
      Enum.find(state.reduce_tasks, fn {_id, t} -> t.assigned_to == :worker1@localhost end)

    for _ <- 0..4 do
      state = GenServer.call({MrMaster.Master, node()}, :get_state)

      # Complete every task assigned to worker 2
      {w2_task_id, _w2_task} =
        Enum.find(state.reduce_tasks, fn {_id, t} ->
          t.assigned_to == :worker2@localhost and t.status == :in_progress
        end)

      GenServer.cast({MrMaster.Master, node()}, {:reduce_done, w2_task_id, w2.node})
    end

    Process.send(GenServer.whereis(MrMaster.Master), :check_stragglers, [])
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.reduce_tasks[w1_task_id].status == :in_progress
    assert Map.has_key?(state.backup_tasks, {:reduce, w1_task_id})
    assert state.backup_tasks[{:reduce, w1_task_id}] == w2.node
  end

  @tag :tmp_dir
  test "phase rolls back to mapping when map workers die during reduce phase", %{tmp_dir: tmp_dir} do
    create_files(tmp_dir, 6)
    w1 = worker_info(:worker1@localhost)
    w2 = worker_info(:worker2@localhost)

    GenServer.call({MrMaster.Master, node()}, {:register, w1})
    GenServer.call({MrMaster.Master, node()}, {:register, w2})

    GenServer.call(
      {MrMaster.Master, node()},
      {:submit_job, job_opts(tmp_dir, task_module: :fake_task3, num_reducers: 2)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    Enum.each(state.map_tasks, fn {task_id, map_task = %MrProtocol.MapTask{}} ->
      locs = bucket_locations(map_task.assigned_to, state.num_reducers)
      GenServer.cast({MrMaster.Master, node()}, {:map_done, task_id, map_task.assigned_to, locs})
    end)

    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.phase == :reducing

    # Before killing the node, build a couple of lists:
    # affected ids => reduce tasks whose locations point to the dead node
    # w1_map_ids => map tasks assigned to w1
    # w2_map_ids => map tasks assigned to w2

    affected_ids =
      state.reduce_tasks
      |> Enum.filter(fn {_id, t} -> Enum.any?(t.locations, fn {n, _} -> n == w2.node end) end)
      |> Enum.map(fn {id, _} -> id end)

    assert affected_ids != []

    w1_map_ids =
      state.map_tasks
      |> Enum.filter(fn {_id, t} -> t.assigned_to == w1.node end)
      |> Enum.map(fn {id, _} -> id end)

    w2_map_ids =
      state.map_tasks
      |> Enum.filter(fn {_id, t} -> t.assigned_to == w2.node end)
      |> Enum.map(fn {id, _} -> id end)

    assert w2_map_ids != []
    Process.send(GenServer.whereis(MrMaster.Master), {:nodedown, w2.node}, [])
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    # After killing node, we should revert to the mapping phase. We should see that all
    # affected reduce tasks are idle and stripped of their previous locations. We should
    # also see that the map tasks assigned to the killed worker are idle/unassigned where the
    # other map tasks stay completed with their previous assignee.
    assert state.phase == :mapping

    assert Enum.all?(affected_ids, fn id ->
             t = state.reduce_tasks[id]
             t.status == :idle and t.locations == []
           end)

    assert Enum.all?(w2_map_ids, fn id ->
             t = state.map_tasks[id]
             t.status == :idle and t.assigned_to == nil
           end)

    assert Enum.all?(w1_map_ids, fn id ->
             t = state.map_tasks[id]
             t.status == :completed and t.assigned_to == w1.node
           end)
  end

  @tag :tmp_dir
  test "set_throttle updates throttle_multiplier in the worker registry", %{tmp_dir: tmp_dir} do
    create_files(tmp_dir, 1)
    w1 = worker_info(:worker1@localhost)
    GenServer.call({MrMaster.Master, node()}, {:register, w1})

    GenServer.call(
      {MrMaster.Master, node()},
      {:submit_job, job_opts(tmp_dir, task_module: :fake_task3)}
    )

    GenServer.cast(
      {MrMaster.Master, node()},
      {:set_throttle, w1.node, 0.5}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.workers[w1.node].throttle_multiplier == 0.5
  end

  # Helper functions to reduce duplication

  defp worker_info(node, coords \\ {0.1, 2.3}) do
    %MrProtocol.WorkerInfo{node: node, coords: coords}
  end

  defp job_opts(tmp_dir, opts \\ []) do
    %{
      input_dir: tmp_dir,
      num_reducers: Keyword.get(opts, :num_reducers, 1),
      task_module: Keyword.get(opts, :task_module, :fake_task)
    }
  end

  defp create_files(tmp_dir, count) do
    for i <- 1..count do
      File.write!(Path.join(tmp_dir, "file#{i}"), "")
    end
  end

  defp bucket_locations(worker_node, num_reducers) do
    worker_id = worker_node |> Atom.to_string() |> String.split("@") |> List.first()

    for bucket <- 0..(num_reducers - 1), into: %{} do
      {bucket, "/tmp/#{worker_id}/bucket-#{bucket}.bin"}
    end
  end
end
