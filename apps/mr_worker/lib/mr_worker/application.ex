defmodule MrWorker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      if Application.get_env(:mr_master, :start_master, false) == false do
        master_node = Application.fetch_env!(:mr_worker, :master_node)
        coords = Application.fetch_env!(:mr_worker, :coords)

        [
          MrWorker.FileServer,
          {MrWorker.Worker, [master_node: master_node, coords: coords]}
        ]
      else
        []
      end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: MrWorker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
