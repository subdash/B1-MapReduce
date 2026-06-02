defmodule MrWorker.FileServer do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def handle_call({:fetch, file_path}, from, state) do
    # Read the file in a spawned task and reply out of band (GenServer.reply + {:noreply, ...})
    # rather than reading inline. This keeps the single FileServer from blocking on one large
    # read, so concurrent fetches from multiple reduce workers run in parallel instead of
    # queuing behind each other. The task is unlinked so a read failure can't take down the server.
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
