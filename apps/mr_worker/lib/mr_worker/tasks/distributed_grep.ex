defmodule MrWorker.Tasks.DistributedGrep do
  @behaviour MrProtocol.Task
  # Hardcoded pattern to grep for -- a future improvement would allow
  # configuring the grep pattern when submitting the job.
  @pattern "the"

  @impl MrProtocol.Task
  def map(filename, line) do
    if String.contains?(String.downcase(line), String.downcase(@pattern)) do
      [{filename, line}]
    else
      []
    end
  end

  @impl MrProtocol.Task
  def reduce(filename, lines) do
    {filename, Enum.join(lines, " | ")}
  end
end
