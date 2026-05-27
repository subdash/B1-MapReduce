import Config

import_config "#{config_env()}.exs"

config :mr_master,
  start_master: System.get_env("MR_START_MASTER", "false") == "true"

coords_str = System.get_env("MR_COORDS", "0.5,0.3")
[x, y] = coords_str |> String.split(",") |> Enum.map(&String.to_float/1)

config :mr_worker,
  master_node: :"master@127.0.0.1",
  coords: {x, y}
