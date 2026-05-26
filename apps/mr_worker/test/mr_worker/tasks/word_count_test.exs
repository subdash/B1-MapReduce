ExUnit.start()

defmodule WordCountTest do
  use ExUnit.Case

  test "map, reduce and combine produce the expected output for a normal sentence" do
    input =
      "A chicken is chicken when it comes to eating chicken because a chicken eating chicken is a cannibal"

    counts = MrWorker.Tasks.WordCount.map("fake", input)

    assert counts ==
             [
               {"A", 1},
               {"chicken", 1},
               {"is", 1},
               {"chicken", 1},
               {"when", 1},
               {"it", 1},
               {"comes", 1},
               {"to", 1},
               {"eating", 1},
               {"chicken", 1},
               {"because", 1},
               {"a", 1},
               {"chicken", 1},
               {"eating", 1},
               {"chicken", 1},
               {"is", 1},
               {"a", 1},
               {"cannibal", 1}
             ]

    chicken_counts =
      Enum.map(counts, fn {word, count} -> if word == "chicken", do: count, else: 0 end)

    assert MrWorker.Tasks.WordCount.reduce("chicken", chicken_counts) == {"chicken", 5}
    assert MrWorker.Tasks.WordCount.combine("chicken", chicken_counts) == {"chicken", 5}
  end

  test "map correctly handles an empty line as input" do
    input = ""
    counts = MrWorker.Tasks.WordCount.map("fake", input)
    assert counts == []
  end
end
