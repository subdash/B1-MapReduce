defmodule MrMasterTest do
  use ExUnit.Case
  doctest MrMaster

  test "greets the world" do
    assert MrMaster.hello() == :world
  end
end
