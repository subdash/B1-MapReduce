defmodule MrWorkerTest do
  use ExUnit.Case
  doctest MrWorker

  test "greets the world" do
    assert MrWorker.hello() == :world
  end
end
