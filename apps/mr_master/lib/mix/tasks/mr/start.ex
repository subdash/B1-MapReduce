defmodule Mix.Tasks.Mr.Start do
  require Logger
  use Mix.Task

  @impl Mix.Task
  def run(args) do
    defaults = [
      workers: 4,
      task: "word_count",
      input: "sample-data/",
      reducers: 4
    ]

    {parsed, _argv, _errors} =
      OptionParser.parse(args,
        strict: [workers: :integer, reducers: :integer, task: :string, input: :string]
      )

    options = Keyword.merge(defaults, parsed)

    {:ok, workers} = Keyword.fetch(options, :workers)
    {:ok, task_string} = Keyword.fetch(options, :task)
    {:ok, input} = Keyword.fetch(options, :input)
    {:ok, reducers} = Keyword.fetch(options, :reducers)

    task_module =
      case task_string do
        "word_count" -> MrWorker.Tasks.WordCount
        "distributed_grep" -> MrWorker.Tasks.DistributedGrep
        # "inverted_index" -> MrWorker.Tasks.InvertedIndex
        _ -> raise "Unknown task: #{task_string}"
      end

    # Pre-compile the project to avoid lock contention
    Logger.info("Pre-compiling project...")

    case Mix.Task.run("compile") do
      {result, _} when result in [:ok, :noop] -> :ok
      result when result in [:ok, :noop] -> :ok
      other -> raise "Pre-compilation failed: #{inspect(other)}"
    end

    Logger.info("Pre-compilation successful")

    # Record start time to report duration at the end
    start_time = System.monotonic_time(:millisecond)

    # Start Erlang distribution in this process as the master node
    master_node = Application.fetch_env!(:mr_master, :master_node)
    cookie = Application.fetch_env!(:mr_master, :cookie)

    case :net_kernel.start([master_node, :longnames]) do
      {:ok, _} ->
        Logger.info("[master] Erlang distribution started: #{node()}")

      {:error, reason} ->
        raise "Failed to start Erlang distribution: #{inspect(reason)}"
    end

    Node.set_cookie(cookie)

    # Start the master GenServer locally
    case MrMaster.Master.start_link([]) do
      {:ok, pid} ->
        Logger.info("[master] Master GenServer started at #{inspect(pid)}")

      {:error, reason} ->
        raise "Failed to start Master GenServer: #{inspect(reason)}"
    end

    # Start the LiveView dashboard alongside the master in the same BEAM VM.
    # `server: true` must be set explicitly — Phoenix only binds a port automatically
    # when started via `mix phx.server`. Without it the app starts but listens nowhere.
    endpoint_config = Application.get_env(:mr_dashboard, MrDashboardWeb.Endpoint, [])

    Application.put_env(
      :mr_dashboard,
      MrDashboardWeb.Endpoint,
      Keyword.put(endpoint_config, :server, true)
    )

    case Application.ensure_all_started(:mr_dashboard) do
      {:ok, _apps} ->
        Logger.info("[master] Dashboard started — http://localhost:4000")

      {:error, reason} ->
        Logger.warning(
          "[master] Dashboard failed to start: #{inspect(reason)} — continuing without it"
        )
    end

    # Start worker processes — keep port handles so we can kill workers on shutdown
    ports =
      Enum.map(1..workers, fn worker_num ->
        worker_name = "worker#{worker_num}@127.0.0.1"
        x = Enum.random(0..99) * 1.0
        y = Enum.random(0..99) * 1.0
        env_vars = "MR_START_MASTER=false MR_COORDS=#{x},#{y}"

        command =
          "sh -c '#{env_vars} elixir --name #{worker_name} --cookie #{cookie} -S mix run --no-compile --no-halt >> worker-#{worker_num}.log 2>&1'"

        Port.open({:spawn, command}, [])
      end)

    wait_for_workers(workers)

    # Verify workers actually connected
    worker_count = GenServer.call(MrMaster.Master, :get_workers) |> map_size()

    if worker_count < workers do
      raise "Only #{worker_count}/#{workers} workers connected"
    end

    # Wrap job execution in try/after so shutdown_workers always runs — even
    # if the job crashes. Without this, a mid-run exception leaves workers alive
    # and their node names stuck in epmd, causing "name in use" on the next run.
    try do
      GenServer.call(
        MrMaster.Master,
        {:submit_job, %{input_dir: input, task_module: task_module, num_reducers: reducers}}
      )

      # Wait for job completion
      wait_for_completion()
      # Report summary
      end_time = System.monotonic_time(:millisecond)
      duration_ms = end_time - start_time
      duration_sec = duration_ms / 1000

      output_files =
        case File.ls("output/") do
          {:ok, files} -> Enum.join(files, ", ")
          _ -> "(no output directory found)"
        end

      Logger.info("=== MapReduce Job Complete ===")
      Logger.info("Duration: #{Float.round(duration_sec, 1)} seconds")
      Logger.info("Results written to output/: #{output_files}")
    after
      shutdown_workers(ports)
    end

    Logger.info("Dashboard still available at http://localhost:4000 — Ctrl+C to exit")
    Process.sleep(:infinity)
  end

  # Gracefully shut down all worker nodes so their names are freed in epmd before
  # the master exits. Uses OTP's :init.stop/0 via RPC as the primary mechanism
  # (clean deregistration), then closes the OS-level ports as a fallback for any
  # workers that crashed and are no longer reachable via Erlang distribution.
  defp shutdown_workers(ports) do
    connected = Node.list()
    Logger.info("[master] Shutting down #{length(connected)} worker node(s)...")

    Enum.each(connected, fn node ->
      :rpc.cast(node, :init, :stop, [])
    end)

    # Give workers time to deregister from epmd before we exit
    Process.sleep(2_000)

    # Fallback: close OS-level ports for any workers that didn't respond
    Enum.each(ports, fn port ->
      if Port.info(port) != nil, do: Port.close(port)
    end)

    Logger.info("[master] Workers shut down.")
  end

  defp poll_until(check_fn, timeout_ms) do
    retry_sleep_ms = 100
    start_time = System.monotonic_time(:millisecond)

    Stream.repeatedly(fn ->
      try do
        case check_fn.() do
          :ok ->
            :ok

          _ ->
            Process.sleep(retry_sleep_ms)
            :retry
        end
      catch
        :exit, _ ->
          Process.sleep(retry_sleep_ms)
          :retry
      end
    end)
    |> Stream.take_while(fn result ->
      elapsed = System.monotonic_time(:millisecond) - start_time
      result != :ok && elapsed < timeout_ms
    end)
    |> Stream.run()
  end

  defp wait_for_workers(expected_count, timeout_ms \\ 30_000) do
    poll_until(
      fn ->
        case GenServer.call(MrMaster.Master, :get_workers) do
          workers when map_size(workers) >= expected_count -> :ok
          _ -> :retry
        end
      end,
      timeout_ms
    )
  end

  defp wait_for_completion(timeout_ms \\ 300_000) do
    poll_until(
      fn ->
        case GenServer.call(MrMaster.Master, :get_phase) do
          :done -> :ok
          _ -> :retry
        end
      end,
      timeout_ms
    )
  end
end
