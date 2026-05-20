defmodule MrMaster.Job do
  # Build list of map tasks by listing files, sorting them,
  # attaching sequential ids and wrapping them in MapTask
  def create_map_tasks(input_dir, num_reducers) do
    files = File.ls!(input_dir)
    sorted = Enum.sort(files)

    Enum.with_index(sorted, fn file_name, index ->
      %MrProtocol.MapTask{
        id: index + 1,
        file_path: Path.join(input_dir, file_name),
        num_reducers: num_reducers
      }
    end)
  end
end
