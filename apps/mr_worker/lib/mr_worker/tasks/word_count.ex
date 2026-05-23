defmodule MrWorker.Task.WordCount do
  def map(filename, line) do
  end

  def reduce(word, counts) do
    {word, Enum.sum(counts)}
  end

  def combine(word, counts) do
    reduce(word, counts)
  end
end
