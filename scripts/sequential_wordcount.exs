#!/usr/bin/env elixir
# Sequential word count script (no MapReduce framework)
# Reads files one at a time, counts words, and merges results
# Records total elapsed time

defmodule SequentialWordCount do
  def run do
    start_time = System.monotonic_time(:millisecond)

    sample_dir = "sample-data"

    # Get all txt files and sort them
    files =
      sample_dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".txt"))
      |> Enum.sort()

    IO.puts("Processing #{length(files)} files from #{sample_dir}/\n")

    # Process each file and aggregate word counts
    word_counts =
      files
      |> Enum.reduce(%{}, fn file, acc ->
        file_path = Path.join(sample_dir, file)
        IO.write("  #{file}... ")

        file_counts = count_file(file_path)

        IO.puts("#{map_size(file_counts)} unique words")

        # Merge file counts into accumulator
        Map.merge(acc, file_counts, fn _key, count1, count2 ->
          count1 + count2
        end)
      end)

    elapsed_ms = System.monotonic_time(:millisecond) - start_time
    elapsed_sec = elapsed_ms / 1000

    IO.puts("\n=== Results ===")
    IO.puts("Total unique words: #{map_size(word_counts)}")
    IO.puts("Total words counted: #{Enum.sum(Map.values(word_counts))}")
    IO.puts("Elapsed time: #{Float.round(elapsed_sec, 2)}s\n")

    # Write results to output file
    output_path = "sequential_output.txt"

    word_counts
    |> Enum.sort_by(fn {_word, count} -> count end, :desc)
    |> write_output(output_path)

    IO.puts("Results written to #{output_path}")
  end

  defp count_file(file_path) do
    case File.read(file_path) do
      {:ok, content} ->
        # File fits in memory, process as a whole
        content
        |> String.split()
        |> Enum.reduce(%{}, fn word, acc ->
          word = String.downcase(word)
          Map.update(acc, word, 1, &(&1 + 1))
        end)

      {:error, :enoent} ->
        IO.puts("File not found: #{file_path}")
        %{}

      {:error, reason} ->
        # Fall back to line-by-line reading for large files
        IO.write("\n    (reading line-by-line due to #{reason})... ")
        count_file_streaming(file_path)
    end
  end

  defp count_file_streaming(file_path) do
    file_path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.filter(&(&1 != ""))
    |> Enum.reduce(%{}, fn line, acc ->
      line
      |> String.split()
      |> Enum.reduce(acc, fn word, inner_acc ->
        word = String.downcase(word)
        Map.update(inner_acc, word, 1, &(&1 + 1))
      end)
    end)
  end

  defp write_output(sorted_words, output_path) do
    content =
      sorted_words
      |> Enum.map(fn {word, count} -> "#{word} #{count}\n" end)
      |> Enum.join()

    File.write!(output_path, content)
  end
end

SequentialWordCount.run()
