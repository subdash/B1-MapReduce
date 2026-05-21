ExUnit.start()

defmodule MasterTest do
  use ExUnit.Case, async: false

  setup do
    case start_supervised(MrMaster.Master) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  test "a registered worker appears in the worker list" do
    worker_info = %MrProtocol.WorkerInfo{
      :node => :worker1@localhost,
      :coords => {0.1, 2.3}
    }

    # node() gives us the master node name
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
end
