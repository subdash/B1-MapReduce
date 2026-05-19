defmodule MrProtocolTest do
  use ExUnit.Case
  doctest MrProtocol

  test "greets the world" do
    assert MrProtocol.hello() == :world
  end
end
