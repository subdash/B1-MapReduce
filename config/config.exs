import Config

import_config "#{config_env()}.exs"

config :mr_worker,
  master_node: :master@localhost,
  coords: {0.5, 0.3}
