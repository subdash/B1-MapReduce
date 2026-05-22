defmodule MasterTest do
  use ExUnit.Case, async: false

  setup do
    start_supervised!(MrMaster.Master)
    :ok
  end

  test "a registered worker appears in the worker list" do
    worker_info = %MrProtocol.WorkerInfo{
      :node => :worker1@localhost,
      :coords => {0.1, 2.3}
    }

    # target the named process on the current node (obtained by calling node()) 
    GenServer.call({MrMaster.Master, node()}, {:register, worker_info})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    registered_worker =
      Enum.find(state.workers, fn {node, _worker} -> node == :worker1@localhost end)

    assert registered_worker != nil
  end

  @tag :tmp_dir
  test "submitting a job creates tasks and assigns them to idle workers", %{tmp_dir: tmp_dir} do
    create_file = fn index ->
      path = Path.join(tmp_dir, "file#{index}")
      File.write!(path, "")
    end

    for index <- 1..10, do: create_file.(index)

    worker_info = %MrProtocol.WorkerInfo{
      :node => :worker1@localhost,
      :coords => {0.1, 2.3}
    }

    GenServer.call({MrMaster.Master, node()}, {:register, worker_info})

    job_opts = %{
      :input_dir => tmp_dir,
      :num_reducers => 1,
      :task_module => :fake_task
    }

    GenServer.call({MrMaster.Master, node()}, {:submit_job, job_opts})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.workers[worker_info.node].status == :busy
    assert map_size(state.map_tasks) == 10
  end

  @tag :tmp_dir
  test "reduce tasks are created with the correct locations after map completion", %{
    tmp_dir: tmp_dir
  } do
    # Create a single test file
    file_path = Path.join(tmp_dir, "single_file")
    File.write!(file_path, "")
    # And a single worker
    worker_info = %MrProtocol.WorkerInfo{
      :node => :worker1@localhost,
      :coords => {0.1, 2.3}
    }

    # And job options
    job_opts = %{
      :input_dir => tmp_dir,
      :num_reducers => 1,
      :task_module => :fake_task
    }

    # Register worker and submit the job
    GenServer.call({MrMaster.Master, node()}, {:register, worker_info})
    GenServer.call({MrMaster.Master, node()}, {:submit_job, job_opts})
    # Get state
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.phase == :mapping
    # Get task id from created map tasks
    [{task_id, task}] = Map.to_list(state.map_tasks)
    # Get the worker and worker id
    worker_node = task.assigned_to
    worker_id = worker_node |> Atom.to_string() |> String.split("@") |> List.first()
    # Construct bucket locations as a map of bucket index to file path
    bucket_locations =
      for bucket <- 0..(state.num_reducers - 1), into: %{} do
        bucket_path = "/tmp/#{worker_id}/bucket-#{bucket}.bin"
        {bucket, bucket_path}
      end

    # Tell server mapping is done
    GenServer.cast({MrMaster.Master, node()}, {:map_done, task_id, bucket_locations})
    # Synchronize state: the cast is async, so a synchronous call after ensures the
    # cast has been processed before we read state
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    # Verify that we are in the reducing phase, we have one reduce task, it is in bucket 0,
    # and the map worker's intermediate file location is recorded in the reduce task. 
    assert state.phase == :reducing
    assert map_size(state.reduce_tasks) == 1
    [{0, reduce_task}] = Map.to_list(state.reduce_tasks)
    assert reduce_task.bucket == 0
    assert {worker_node, bucket_locations[0]} in reduce_task.locations
  end

  @tag :tmp_dir
  test "phase transitions to done after a full MapReduce job", %{
    tmp_dir: tmp_dir
  } do
    file_path = Path.join(tmp_dir, "single_file")
    File.write!(file_path, "")

    worker_info = %MrProtocol.WorkerInfo{
      :node => :worker1@localhost,
      :coords => {0.1, 2.3}
    }

    job_opts = %{
      :input_dir => tmp_dir,
      :num_reducers => 1,
      :task_module => :fake_task2
    }

    GenServer.call({MrMaster.Master, node()}, {:register, worker_info})
    GenServer.call({MrMaster.Master, node()}, {:submit_job, job_opts})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    [{task_id, task}] = Map.to_list(state.map_tasks)
    worker_node = task.assigned_to
    worker_id = worker_node |> Atom.to_string() |> String.split("@") |> List.first()

    bucket_locations =
      for bucket <- 0..(state.num_reducers - 1), into: %{} do
        bucket_path = "/tmp/#{worker_id}/bucket-#{bucket}.bin"
        {bucket, bucket_path}
      end

    GenServer.cast({MrMaster.Master, node()}, {:map_done, task_id, bucket_locations})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    [{task_id, _task}] = Map.to_list(state.reduce_tasks)
    GenServer.cast({MrMaster.Master, node()}, {:reduce_done, task_id})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.phase == :done
  end

  @tag :tmp_dir
  test "phase remains reducing while some tasks are still in progress", %{tmp_dir: tmp_dir} do
    file_path = Path.join(tmp_dir, "single_file")
    File.write!(file_path, "")

    worker_info = %MrProtocol.WorkerInfo{
      node: :worker1@localhost,
      coords: {0.1, 2.3}
    }

    job_opts = %{
      input_dir: tmp_dir,
      num_reducers: 2,
      task_module: :fake_task3
    }

    GenServer.call({MrMaster.Master, node()}, {:register, worker_info})
    GenServer.call({MrMaster.Master, node()}, {:submit_job, job_opts})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    # Complete the single map task
    [{task_id, task}] = Map.to_list(state.map_tasks)
    worker_id = task.assigned_to |> Atom.to_string() |> String.split("@") |> List.first()

    bucket_locations =
      for bucket <- 0..(state.num_reducers - 1), into: %{} do
        {bucket, "/tmp/#{worker_id}/bucket-#{bucket}.bin"}
      end

    GenServer.cast({MrMaster.Master, node()}, {:map_done, task_id, bucket_locations})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.phase == :reducing
    assert map_size(state.reduce_tasks) == 2

    # Find the one reduce task that is in_progress (assigned to our worker)
    {reduce_task_id, _} =
      Enum.find(state.reduce_tasks, fn {_id, t} -> t.status == :in_progress end)

    # Complete only that one task — one of two is still pending
    GenServer.cast({MrMaster.Master, node()}, {:reduce_done, reduce_task_id})
    state = GenServer.call({MrMaster.Master, node()}, :get_state)

    assert state.phase == :reducing
  end
end
