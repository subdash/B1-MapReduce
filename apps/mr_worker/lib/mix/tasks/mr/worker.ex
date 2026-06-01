defmodule Mix.Tasks.Mr.Worker do
  require Logger
  use Mix.Task
  @requirements ["app.config"]

  @impl Mix.Task
  def run(args) do
    {options, _argv, _errors} = OptionParser.parse(args, strict: [name: :string])

    name =
      case Keyword.fetch(options, :name) do
        {:ok, name} -> name
        :error -> raise "--name is required, e.g. --name worker1@host"
      end

    cookie = Application.fetch_env!(:mr_worker, :cookie)
    # Start distribution: the BEAM VM (a single Erlang/Elixir OS process) gets a node
    # name, it opens a socket for TCP connections so that other nodes can connect to it
    # and it registers with epmd (Erlang Port Mapper Daemon) which maps node names to
    # the TCP port each node is listening on.
    case :net_kernel.start([String.to_atom(name), :longnames]) do
      {:ok, _} ->
        Logger.info("[worker] Erlang distribution started: #{node()}")

      {:error, reason} ->
        raise "Failed to start Erlang distribution: #{inspect(reason)}"
    end

    # Both nodes must hold the same cookie in order to connect to each other
    Node.set_cookie(cookie)
    # Start worker OTP app -> supervisor boots MrWorker.Worker which connects, registers
    # and retries on its own.
    case Application.ensure_all_started(:mr_worker) do
      {:ok, _} -> :ok
      {:error, {app, reason}} -> raise "Application #{app} did not start: #{inspect(reason)}"
    end

    Process.sleep(:infinity)
  end
end
