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
        # "grep" -> MrWorker.Tasks.Grep
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
    case :net_kernel.start([:"master@127.0.0.1", :longnames]) do
      {:ok, _} ->
        Logger.info("[master] Erlang distribution started: #{node()}")

      {:error, reason} ->
        raise "Failed to start Erlang distribution: #{inspect(reason)}"
    end

    Node.set_cookie(:secret)

    # Start the master GenServer locally
    case MrMaster.Master.start_link([]) do
      {:ok, pid} ->
        Logger.info("[master] Master GenServer started at #{inspect(pid)}")

      {:error, reason} ->
        raise "Failed to start Master GenServer: #{inspect(reason)}"
    end

    # Start worker processes
    Enum.each(1..workers, fn worker_num ->
      worker_name = "worker#{worker_num}@127.0.0.1"
      x = Enum.random(0..99) * 1.0
      y = Enum.random(0..99) * 1.0
      env_vars = "MR_START_MASTER=false MR_COORDS=#{x},#{y}"

      command =
        "sh -c '#{env_vars} elixir --name #{worker_name} --cookie secret -S mix run --no-halt'"

      Port.open({:spawn, command}, [])
    end)

    wait_for_workers(workers)

    # Verify workers actually connected
    worker_count = GenServer.call(MrMaster.Master, :get_workers) |> map_size()

    if worker_count < workers do
      raise "Only #{worker_count}/#{workers} workers connected"
    end

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
