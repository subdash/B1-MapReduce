defmodule MrWorker.FileServer do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def handle_call({:fetch, file_path}, from, state) do
    # Read file and return contents as binary
    Task.start(fn ->
      result =
        case File.read(file_path) do
          {:ok, binary} -> binary
          {:error, :enoent} -> {:error, :not_found}
          _ -> {:error, :unexpected}
        end

      GenServer.reply(from, result)
    end)

    {:noreply, state}
  end
end
