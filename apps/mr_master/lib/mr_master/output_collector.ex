defmodule MrMaster.OutputCollector do
  use GenServer
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_), do: {:ok, %{}}

  # Receive finished output files from reduce workers and write them onto the master's
  # disk. Read: reduce workers send bytes to master instead of writing output to a local
  # path -- a departure from the original MapReduce design we follow.
  def handle_call({:write_output, filename, binary}, _from, state) do
    dir = Application.fetch_env!(:mr_master, :output_base_dir)
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, filename), binary)
    {:reply, :ok, state}
  end
end
