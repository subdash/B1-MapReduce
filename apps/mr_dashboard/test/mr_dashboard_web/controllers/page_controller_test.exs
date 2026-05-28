defmodule MrDashboardWeb.PageControllerTest do
  use MrDashboardWeb.ConnCase

  test "GET / renders the MapReduce dashboard", %{conn: conn} do
    conn = get(conn, ~p"/")
    assert html_response(conn, 200) =~ "MapReduce Dashboard"
  end
end
