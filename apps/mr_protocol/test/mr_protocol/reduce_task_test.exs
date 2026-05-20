ExUnit.start()

defmodule ReduceTaskStructTest do
  use ExUnit.Case, async: true

  test "A new ReduceTask struct has default status of idle" do
    reducetask = %MrProtocol.ReduceTask{}
    assert reducetask.status == :idle
  end

  test "A new ReduceTask struct can be updated using the struct update syntax" do
    reducetask = %MrProtocol.ReduceTask{
      id: 0,
      bucket: 0,
      locations: [{:worker1@localhost, "/path/to/file"}],
      status: :in_progress,
      assigned_to: nil
    }

    reducetask = %{reducetask | status: :completed, bucket: 3}
    assert reducetask.status == :completed
    assert reducetask.bucket == 3
  end
end
