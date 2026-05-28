defmodule MrDashboard.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      MrDashboardWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:mr_dashboard, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: MrDashboard.PubSub},
      # Start a worker by calling: MrDashboard.Worker.start_link(arg)
      # {MrDashboard.Worker, arg},
      # Start to serve requests, typically the last entry
      MrDashboardWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: MrDashboard.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    MrDashboardWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
