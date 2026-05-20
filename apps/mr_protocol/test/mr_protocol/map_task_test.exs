ExUnit.start()

defmodule MapTaskStructTest do
  use ExUnit.Case, async: true

  test "A new MapTask struct has default status of idle" do
    maptask = %MrProtocol.MapTask{}
    assert maptask.status == :idle
  end

  test "A new MapTask struct can be updated using the struct update syntax" do
    maptask = %MrProtocol.MapTask{
      id: 0,
      file_path: "/path/to/file",
      num_reducers: 1,
      status: :in_progress,
      assigned_to: nil
    }

    maptask = %{maptask | status: :dead, num_reducers: 2}
    assert maptask.status == :dead
    assert maptask.num_reducers == 2
  end
end
