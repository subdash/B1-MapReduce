ExUnit.start()

defmodule RPCTest do
  use ExUnit.Case

  test "call/5 correctly calculates and applies delay based on distance" do
    registry = %{
      :worker1@localhost => %MrProtocol.WorkerInfo{
        node: :worker1@localhost,
        coords: {0.0, 0.0}
      },
      node() => %MrProtocol.WorkerInfo{
        node: node(),
        coords: {3.0, 4.0}
      }
    }

    {elapsed_us, _result} =
      :timer.tc(fn ->
        # Wrap call in try since we'll get an error testing against a nil server
        try do
          MrWorker.RPC.call(:worker1@localhost, nil, {:fetch, "path"}, registry)
        catch
          :exit, _ -> :error
        end
      end)

    elapsed_ms = elapsed_us / 1000

    assert elapsed_ms >= 9 and elapsed_ms < 15
  end
end
