ExUnit.start()

defmodule WorkerInfoStructTest do
  use ExUnit.Case, async: true

  test "A new WorkerInfo struct has default status of idle" do
    winfo = %MrProtocol.WorkerInfo{}
    assert winfo.status == :idle
  end

  test "A new WorkerInfo struct can be updated using the struct update syntax" do
    winfo = %MrProtocol.WorkerInfo{
      node: :worker1@localhost,
      coords: {0.1, 2.3},
      status: :busy
    }

    winfo = %{winfo | status: :dead}
    assert winfo.status == :dead
  end
end
