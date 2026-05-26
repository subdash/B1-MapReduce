ExUnit.start()

defmodule SchedulerTest do
  use ExUnit.Case, async: true

  test "assign_map_task returns the first idle worker" do
    coords = {0.0, 0.0}

    workers = %{
      :first => %MrProtocol.WorkerInfo{node: :first, coords: coords, status: :in_progress},
      :second => %MrProtocol.WorkerInfo{node: :second, coords: coords, status: :in_progress},
      :third => %MrProtocol.WorkerInfo{node: :third, coords: coords, status: :idle},
      :fourth => %MrProtocol.WorkerInfo{node: :fourth, coords: coords, status: :idle},
      :fifth => %MrProtocol.WorkerInfo{node: :fifth, coords: coords, status: :in_progress}
    }

    assigned_worker = MrMaster.Scheduler.assign_map_task(workers)

    assert assigned_worker == :third
  end

  test "assign_map_task returns nil when no idle workers" do
    coords = {0.0, 0.0}

    workers = %{
      :first => %MrProtocol.WorkerInfo{node: :first, coords: coords, status: :in_progress},
      :second => %MrProtocol.WorkerInfo{node: :second, coords: coords, status: :in_progress},
      :third => %MrProtocol.WorkerInfo{node: :third, coords: coords, status: :dead}
    }

    assigned_worker = MrMaster.Scheduler.assign_map_task(workers)

    assert assigned_worker == nil
  end

  test "assign_reduce_task picks the closest worker" do
    map_worker_nodes = [:worker1@localhost, :worker2@localhost]

    workers = %{
      :worker1@localhost => %MrProtocol.WorkerInfo{
        node: :worker1@localhost,
        coords: {0.0, 0.0},
        status: :in_progress
      },
      :worker2@localhost => %MrProtocol.WorkerInfo{
        node: :worker2@localhost,
        coords: {10.0, 10.0},
        status: :in_progress
      },
      :worker3@localhost => %MrProtocol.WorkerInfo{
        node: :worker3@localhost,
        coords: {5.0, 5.0},
        status: :idle
      },
      :worker4@localhost => %MrProtocol.WorkerInfo{
        node: :worker4@localhost,
        coords: {15.0, 15.0},
        status: :idle
      }
    }

    assigned_worker = MrMaster.Scheduler.assign_reduce_task(workers, map_worker_nodes)

    assert assigned_worker == :worker3@localhost
  end

  test "assign_reduce_task returns nil when there are no idle workers" do
    map_worker_nodes = [:worker1@localhost, :worker2@localhost]
    coords = {0.0, 0.0}

    workers = %{
      :first => %MrProtocol.WorkerInfo{node: :first, coords: coords, status: :in_progress},
      :second => %MrProtocol.WorkerInfo{node: :second, coords: coords, status: :in_progress},
      :third => %MrProtocol.WorkerInfo{node: :third, coords: coords, status: :dead}
    }

    assigned_worker = MrMaster.Scheduler.assign_reduce_task(workers, map_worker_nodes)

    assert assigned_worker == nil
  end
end
