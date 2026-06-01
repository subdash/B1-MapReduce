defmodule MrWorker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      [MrWorker.FileServer] ++
        if Application.get_env(:mr_worker, :start_worker, true) do
          master_node = Application.fetch_env!(:mr_worker, :master_node)

          coords =
            case Application.get_env(:mr_worker, :coords) do
              nil -> {Enum.random(0..99) * 1.0, Enum.random(0..99) * 1.0}
              c -> c
            end

          [{MrWorker.Worker, [master_node: master_node, coords: coords]}]
        else
          []
        end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: MrWorker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
