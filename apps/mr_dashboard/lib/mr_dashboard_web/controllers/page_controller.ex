defmodule MrDashboardWeb.PageController do
  use MrDashboardWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
