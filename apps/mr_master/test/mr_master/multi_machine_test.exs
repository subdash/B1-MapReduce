defmodule MrMaster.MultiMachineTest do
  use ExUnit.Case

  setup do
    start_supervised!(MrMaster.Master)
    :ok
  end

  @tag :tmp_dir
  test "registering a worker mid-job assigns pending tasks", %{tmp_dir: tmp_dir} do
    create_files(tmp_dir, 6)

    GenServer.call(
      {MrMaster.Master, node()},
      {:submit_job, job_opts(tmp_dir, task_module: :fake_task3, num_reducers: 2)}
    )

    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert state.phase == :mapping
    assert Enum.all?(state.map_tasks, fn {_task_id, task} -> task.assigned_to == nil end)

    w1 = worker_info(:worker1@localhost)

    GenServer.call({MrMaster.Master, node()}, {:register, w1})

    state = GenServer.call({MrMaster.Master, node()}, :get_state)
    assert Enum.count(state.map_tasks, fn {_id, t} -> t.assigned_to == w1.node end) == 1
    assert state.workers[w1.node].status == :busy
  end

  defp create_files(tmp_dir, count) do
    for i <- 1..count do
      File.write!(Path.join(tmp_dir, "file#{i}"), "")
    end
  end

  defp worker_info(node, coords \\ {0.1, 2.3}) do
    %MrProtocol.WorkerInfo{node: node, coords: coords}
  end

  defp job_opts(tmp_dir, opts) do
    %{
      input_dir: tmp_dir,
      num_reducers: Keyword.get(opts, :num_reducers, 1),
      task_module: Keyword.get(opts, :task_module, :fake_task)
    }
  end
end
