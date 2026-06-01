defmodule MrWorker.MapTask do
  require Logger

  def execute(task, task_module, worker_node_name) do
    Logger.info("[map_task] starting | id=#{task.id} file=#{task.file_path}")

    # Read file line by line to build up a list of pairs
    pairs =
      File.stream!(task.file_path)
      |> Enum.reduce([], fn line, acc ->
        word_count_pairs = task_module.map(task.file_path, line)
        word_count_pairs ++ acc
      end)
      |> Enum.reverse()

    # Use hash function to get a bucket name and group results by bucket name 
    grouped_pairs =
      Enum.group_by(pairs, fn {key, _value} -> :erlang.phash2(key, task.num_reducers) end)

    # For every bucket/value list pair:
    bucket_locations =
      Enum.map(grouped_pairs, fn {bucket, pairs} ->
        pairs =
          if function_exported?(task_module, :combine, 2) do
            # Call combine if it is defined
            pairs
            |> Enum.group_by(fn {key, _} -> key end, fn {_, value} -> value end)
            |> Enum.map(fn {key, values} -> task_module.combine(key, values) end)
          else
            pairs
          end

        # Take all pairs, convert to binary
        encoded = :erlang.term_to_binary(pairs)
        # Write to file
        base = Application.fetch_env!(:mr_worker, :temp_base_dir)
        dirname = Path.join(base, to_string(worker_node_name))
        filename = "map-#{task.id}-bucket-#{bucket}.bin"
        filepath = "#{dirname}/#{filename}"
        File.mkdir_p!(dirname)
        File.write!(filepath, encoded)
        {bucket, filepath}
      end)
      |> Map.new()

    Logger.info("[map_task] done | id=#{task.id} buckets=#{map_size(bucket_locations)}")
    {:ok, bucket_locations}
  end
end
