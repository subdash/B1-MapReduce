ExUnit.start()

defmodule MrProtocolTaskBehaviourTest do
  use ExUnit.Case, async: true

  defmodule ExampleTask do
    @behaviour MrProtocol.Task

    def map(key, value) do
      [{key, value}]
    end

    def reduce(key, values) do
      [head | _tail] = values
      {key, head}
    end

    def combine(key, values) do
      [head | _tail] = values
      {key, head}
    end
  end

  test "Verify that the Task module is valid" do
    key = "key"
    value = 1
    values = [1, 2]

    map_result = ExampleTask.map(key, value)
    reduce_result = ExampleTask.reduce(key, values)
    combine_result = ExampleTask.combine(key, values)

    assert map_result == [{key, value}]
    assert reduce_result == {key, 1}
    assert combine_result == {key, 1}
  end
end
