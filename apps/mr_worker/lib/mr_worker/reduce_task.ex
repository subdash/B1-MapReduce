defmodule MrWorker.ReduceTask do
  def execute(task, task_module, registry, output_dir, throttle_multiplier) do
    File.mkdir_p!(output_dir)

    file_contents =
      task.locations
      # List of decoded key/value pairs
      |> Enum.reduce([], fn {node, file_path}, acc ->
        case MrWorker.RPC.call(
               node,
               MrWorker.FileServer,
               {:fetch, file_path},
               registry,
               throttle_multiplier
             ) do
          {:error, reason} ->
            raise "could not fetch file #{file_path} from #{node}: #{inspect(reason)}"

          binary ->
            :erlang.binary_to_term(binary) ++ acc
        end
      end)
      # Sort by key
      |> Enum.sort_by(fn {k, _} -> k end)
      # Group by key
      |> Enum.group_by(fn {k, _} -> k end, fn {_, v} -> v end)
      # Map into list of {key, result}
      |> Enum.map(fn {k, v} -> task_module.reduce(k, v) end)
      # Convert pairs to strings 
      |> Enum.map(fn {key, result} -> "#{key}\t#{result}\n" end)
      # Join into a single string
      |> Enum.join("")

    file_path = "#{output_dir}/bucket-#{task.bucket}.txt"
    File.write!(file_path, file_contents)

    :ok
  end
end
