defmodule MrWorker.FileServer do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def handle_call({:fetch, file_path}, _from, state) do
    # Read file and return contents as binary
    case File.read(file_path) do
      {:ok, binary} -> {:reply, binary, state}
      {:error, :enoent} -> {:reply, {:error, :not_found}, state}
      _ -> {:reply, {:error, :unexpected}, state}
    end
  end
end
