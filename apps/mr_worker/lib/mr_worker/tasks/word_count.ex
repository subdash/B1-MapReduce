defmodule MrWorker.Tasks.WordCount do
  @behaviour MrProtocol.Task

  def map(_filename, line) do
    line |> String.split() |> Enum.map(fn word -> {word, 1} end)
  end

  def reduce(word, counts) do
    {word, Enum.sum(counts)}
  end

  def combine(word, counts) do
    reduce(word, counts)
  end
end
